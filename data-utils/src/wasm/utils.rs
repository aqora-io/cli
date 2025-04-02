use js_sys::{AsyncIterator, IteratorNext, JsString, Object, Uint8Array};
use tokio::io::{self, AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::Blob;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub fn set_console_error_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// gets constructor name if object otherwise typeof
fn js_value_type_name(value: &JsValue) -> JsString {
    if value.is_object() {
        value.unchecked_ref::<Object>().constructor().name()
    } else {
        value.js_typeof().unchecked_into::<JsString>()
    }
}

fn js_value_to_io_error(value: &JsValue) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        format!(
            "Failed to read from stream: {}",
            value
                .as_string()
                .unwrap_or_else(|| "Unknown error".to_string())
        ),
    )
}

/// returns `true` if array is empty after read
fn read_uint8array(array: &mut Uint8Array, buf: &mut ReadBuf<'_>) -> bool {
    let length = array.length();
    let amount = std::cmp::min(length, buf.remaining() as u32);
    array
        .subarray(0, amount)
        .copy_to(buf.initialize_unfilled_to(amount as usize));
    buf.advance(amount as usize);
    *array = array.subarray(amount, length);
    amount == length
}

enum JsAsyncReaderNext {
    Future(JsFuture),
    Bytes(Uint8Array),
}

impl JsAsyncReaderNext {
    fn as_mut_future(&mut self) -> Option<&mut JsFuture> {
        match self {
            Self::Future(future) => Some(future),
            Self::Bytes(_) => None,
        }
    }
}

pub struct JsAsyncReader {
    iter: AsyncIterator,
    next: Option<JsAsyncReaderNext>,
    done: bool,
}

impl JsAsyncReader {
    pub fn new(iter: AsyncIterator) -> Self {
        Self {
            iter,
            next: None,
            done: false,
        }
    }
}

impl AsyncRead for JsAsyncReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.done {
            return Poll::Ready(Ok(()));
        }
        let future = match self.next.as_mut() {
            Some(JsAsyncReaderNext::Future(future)) => future,
            Some(JsAsyncReaderNext::Bytes(bytes)) => {
                if read_uint8array(bytes, buf) {
                    self.next.take();
                }
                return Poll::Ready(Ok(()));
            }
            None => match self.iter.next().map(JsFuture::from) {
                Ok(val) => {
                    self.next = Some(JsAsyncReaderNext::Future(val));
                    self.next.as_mut().unwrap().as_mut_future().unwrap()
                }
                Err(e) => {
                    self.done = true;
                    return Poll::Ready(Err(js_value_to_io_error(&e)));
                }
            },
        };
        match Pin::new(future).poll(cx) {
            Poll::Ready(res) => match res {
                Ok(iter_next) => {
                    let next = iter_next.unchecked_into::<IteratorNext>();
                    if next.done() {
                        self.done = true;
                        Poll::Ready(Ok(()))
                    } else {
                        self.next.take();
                        let mut bytes = match next.value().dyn_into::<Uint8Array>() {
                            Ok(array) => array,
                            Err(value) => {
                                self.done = true;
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!(
                                        "Cannot convert {} to Uint8Array",
                                        js_value_type_name(&value)
                                    ),
                                )));
                            }
                        };
                        if !read_uint8array(&mut bytes, buf) {
                            self.next = Some(JsAsyncReaderNext::Bytes(bytes));
                        }
                        Poll::Ready(Ok(()))
                    }
                }
                Err(e) => {
                    self.done = true;
                    Poll::Ready(Err(js_value_to_io_error(&e)))
                }
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct AsyncBlobReader {
    blob: Blob,
    reader: JsAsyncReader,
    initial_offset: usize,
    bytes_read: usize,
}

impl AsyncBlobReader {
    pub fn new(blob: Blob) -> Self {
        Self {
            reader: JsAsyncReader::new(blob.stream().values()),
            blob,
            initial_offset: 0,
            bytes_read: 0,
        }
    }

    fn offset(&self) -> usize {
        self.initial_offset + self.bytes_read
    }
}

impl AsyncSeek for AsyncBlobReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let length = self.blob.size() as i64;
        let new_offset = match position {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::Current(offset) => self.offset() as i64 + offset,
            SeekFrom::End(offset) => length + offset,
        };
        if new_offset < 0 || new_offset > length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Offset negative or outside boundaries",
            ));
        }
        if new_offset == self.offset() as i64 {
            return Ok(());
        }
        let sliced_blob = self
            .blob
            .slice_with_i32(new_offset as i32)
            .map_err(|e| js_value_to_io_error(&e))?;
        self.reader = JsAsyncReader::new(sliced_blob.stream().values());
        self.initial_offset = new_offset as usize;
        self.bytes_read = 0;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.offset() as u64))
    }
}

impl AsyncRead for AsyncBlobReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.reader).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                self.bytes_read += buf.filled().len();
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

// https://docs.rs/wasm-bindgen-futures/latest/src/wasm_bindgen_futures/stream.rs.html#39-81

// https://developer.mozilla.org/en-US/docs/Web/API/WritableStream/getWriter
// https://developer.mozilla.org/en-US/docs/Web/API/TransformStream
// https://stackoverflow.com/questions/14269233/node-js-how-to-read-a-stream-into-a-buffer
