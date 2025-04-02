use js_sys::{IteratorNext, JsString, Object, Uint8Array};
use tokio::io::{self, AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Blob, FileReaderSync, ReadableStream, ReadableStreamDefaultReader};

use std::future::Future;
use std::io::{Read, Seek};
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
    reader: ReadableStreamDefaultReader,
    next: Option<JsAsyncReaderNext>,
    done: bool,
}

impl JsAsyncReader {
    pub fn new(reader: ReadableStreamDefaultReader) -> Self {
        Self {
            reader,
            next: None,
            done: false,
        }
    }
}

impl From<ReadableStream> for JsAsyncReader {
    fn from(stream: ReadableStream) -> Self {
        Self::new(
            stream
                .get_reader()
                .dyn_into()
                .expect("Get reader should return ReadableStreamDefaultReader"),
        )
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
            None => {
                self.next = Some(JsAsyncReaderNext::Future(JsFuture::from(
                    self.reader.read(),
                )));
                self.next.as_mut().unwrap().as_mut_future().unwrap()
            }
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

pub struct SeekableBlob {
    blob: Blob,
    sliced_blob: Option<Blob>,
    offset: u64,
}

impl SeekableBlob {
    pub fn new(blob: Blob) -> Self {
        Self {
            blob,
            sliced_blob: None,
            offset: 0,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn blob(&self) -> &Blob {
        self.sliced_blob.as_ref().unwrap_or(&self.blob)
    }

    pub fn into_inner(self) -> Blob {
        self.blob
    }
}

pub fn shift_position(position: SeekFrom, amt: i64) -> SeekFrom {
    match position {
        SeekFrom::Start(n) => SeekFrom::Start(n),
        SeekFrom::End(n) => SeekFrom::End(n),
        SeekFrom::Current(n) => SeekFrom::Current(n + amt),
    }
}

impl Seek for SeekableBlob {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let length = self.blob.size() as u64;
        let (base_pos, offset) = match position {
            SeekFrom::Start(n) => (0u64, n as i64),
            SeekFrom::End(n) => (length, n),
            SeekFrom::Current(n) => (self.offset, n),
        };
        let offset = match base_pos.checked_add_signed(offset) {
            Some(n) => n,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ));
            }
        };
        if offset == self.offset() {
            return Ok(offset);
        }
        if offset == 0 {
            self.sliced_blob = None;
        } else {
            self.sliced_blob = Some(
                self.blob
                    .slice_with_i32(offset as i32)
                    .map_err(|e| js_value_to_io_error(&e))?,
            );
        }
        self.offset = offset;
        Ok(offset)
    }
}

pub struct AsyncBlobReader {
    blob: SeekableBlob,
    reader: JsAsyncReader,
    bytes_read: usize,
}

impl AsyncBlobReader {
    pub fn new(blob: Blob) -> Self {
        Self {
            reader: JsAsyncReader::from(blob.stream()),
            blob: SeekableBlob::new(blob),
            bytes_read: 0,
        }
    }

    pub fn offset(&self) -> u64 {
        self.blob.offset() + self.bytes_read as u64
    }

    pub fn into_inner(self) -> Blob {
        self.blob.into_inner()
    }
}

impl AsyncSeek for AsyncBlobReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let shifted = shift_position(position, self.bytes_read as i64);
        self.blob.seek(shifted)?;
        self.reader = JsAsyncReader::from(self.blob.blob().stream());
        self.bytes_read = 0;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.offset()))
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

pub struct BlobReader {
    blob: SeekableBlob,
    reader: Option<FileReaderSync>,
    bytes_read: usize,
}

impl BlobReader {
    pub fn new(blob: Blob) -> Self {
        Self {
            blob: SeekableBlob::new(blob),
            reader: None,
            bytes_read: 0,
        }
    }

    pub fn offset(&self) -> u64 {
        self.blob.offset() + self.bytes_read as u64
    }
}

impl Seek for BlobReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let shifted = shift_position(position, self.bytes_read as i64);
        self.blob.seek(shifted)?;
        self.bytes_read = 0;
        Ok(self.offset())
    }
}

impl Read for BlobReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let blob = self.blob.blob();
        let size = std::cmp::min(blob.size() as usize - self.bytes_read, buf.len());
        let sliced_blob = blob
            .slice_with_i32_and_i32(self.bytes_read as i32, (self.bytes_read + size) as i32)
            .map_err(|err| js_value_to_io_error(&err))?;
        let reader = if let Some(reader) = self.reader.as_ref() {
            reader
        } else {
            self.reader = Some(FileReaderSync::new().map_err(|err| js_value_to_io_error(&err))?);
            self.reader.as_ref().unwrap()
        };
        let buffer = reader
            .read_as_array_buffer(&sliced_blob)
            .map_err(|err| js_value_to_io_error(&err))?;
        Uint8Array::new(&buffer).copy_to(&mut buf[..size]);
        self.bytes_read += size;
        Ok(size)
    }
}

// https://docs.rs/wasm-bindgen-futures/latest/src/wasm_bindgen_futures/stream.rs.html#39-81

// https://developer.mozilla.org/en-US/docs/Web/API/WritableStream/getWriter
// https://developer.mozilla.org/en-US/docs/Web/API/TransformStream
// https://stackoverflow.com/questions/14269233/node-js-how-to-read-a-stream-into-a-buffer
