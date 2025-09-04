use bytes::BytesMut;
use futures::prelude::*;
use js_sys::Uint8Array;
use std::cell::Cell;
use std::io::{Read, Seek};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use wasm_bindgen::prelude::*;
use web_sys::{Blob, FileReaderSync};

use super::cast::JsCastExt;
use super::error::WasmError;

#[wasm_bindgen(js_name = "isByobReaderSupported")]
pub fn is_byob_reader_supported() -> bool {
    thread_local! {
        static IS_BYOB_READER_SUPPORTED: Cell<Option<bool>> = const { Cell::new(None) };
    }
    if let Some(supported) = IS_BYOB_READER_SUPPORTED.get() {
        supported
    } else {
        let options = web_sys::ReadableStreamGetReaderOptions::new();
        options.set_mode(web_sys::ReadableStreamReaderMode::Byob);
        let supported =
            web_sys::ReadableStreamByobReader::new(&web_sys::Blob::new().unwrap().stream()).is_ok();
        IS_BYOB_READER_SUPPORTED.set(Some(supported));
        supported
    }
}

pub fn async_read_to_readable_stream<R>(
    async_read: R,
    default_buffer_len: usize,
) -> web_sys::ReadableStream
where
    R: AsyncRead + 'static,
{
    if is_byob_reader_supported() {
        wasm_streams::ReadableStream::from_async_read(async_read.compat(), default_buffer_len)
    } else {
        wasm_streams::ReadableStream::from_stream(
            tokio_util::io::ReaderStream::new(async_read)
                .map_ok(|bytes| JsValue::from(js_sys::Uint8Array::from(bytes.as_ref())))
                .map_err(|err| JsValue::from(JsError::from(err))),
        )
    }
    .into_raw()
}

pub type BoxAsyncRead = Box<dyn AsyncRead + Unpin>;

pub fn readable_stream_to_async_read(readable_stream: web_sys::ReadableStream) -> BoxAsyncRead {
    let readable_stream = wasm_streams::ReadableStream::from_raw(readable_stream);
    if is_byob_reader_supported() {
        Box::new(readable_stream.into_async_read().compat())
    } else {
        Box::new(tokio_util::io::StreamReader::new(
            readable_stream.into_stream().map(|item| {
                match item.map_err(WasmError::from)?.cast_into::<Uint8Array>() {
                    Ok(array) => {
                        let size = array.length() as usize;
                        let mut bytes = BytesMut::with_capacity(size);
                        let uninit = &mut bytes.spare_capacity_mut()[..size];
                        let len = array.copy_to_uninit(uninit).len();
                        unsafe { bytes.set_len(len) };
                        Ok(bytes)
                    }
                    Err(err) => Err(WasmError::from(err)),
                }
            }),
        ))
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
                    .slice_with_f64(offset as f64)
                    .map_err(WasmError::from)?,
            );
        }
        self.offset = offset;
        Ok(offset)
    }
}

pub struct AsyncBlobReader {
    blob: SeekableBlob,
    reader: BoxAsyncRead,
    bytes_read: u64,
}

impl AsyncBlobReader {
    pub fn new(blob: Blob) -> Self {
        Self {
            reader: readable_stream_to_async_read(blob.stream()),
            blob: SeekableBlob::new(blob),
            bytes_read: 0,
        }
    }

    pub fn offset(&self) -> u64 {
        self.blob.offset() + self.bytes_read
    }

    pub fn into_inner(self) -> Blob {
        self.blob.into_inner()
    }
}

impl AsyncSeek for AsyncBlobReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let shifted = shift_position(position, self.bytes_read as i64);
        self.blob.seek(shifted)?;
        self.reader = readable_stream_to_async_read(self.blob.blob().stream());
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
        let already_filled = buf.filled().len();
        match Pin::new(&mut self.reader).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                self.bytes_read += (buf.filled().len() - already_filled) as u64;
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
    bytes_read: u64,
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
        self.blob.offset() + self.bytes_read
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
        let Some(remaining) = (blob.size() as u64).checked_sub(self.bytes_read) else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected EOF",
            ));
        };
        let size: usize = std::cmp::min(remaining.try_into().unwrap_or(usize::MAX), buf.len());
        let sliced_blob = blob
            .slice_with_f64_and_f64(
                self.bytes_read as f64,
                (self.bytes_read + size as u64) as f64,
            )
            .map_err(WasmError::from)?;
        let reader = if let Some(reader) = self.reader.as_ref() {
            reader
        } else {
            self.reader = Some(FileReaderSync::new().map_err(WasmError::from)?);
            self.reader.as_ref().unwrap()
        };
        let buffer = reader
            .read_as_array_buffer(&sliced_blob)
            .map_err(WasmError::from)?;
        Uint8Array::new(&buffer).copy_to(&mut buf[..size]);
        self.bytes_read += size as u64;
        Ok(size)
    }
}
