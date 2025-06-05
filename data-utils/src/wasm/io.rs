use js_sys::Uint8Array;
use std::io::{Read, Seek};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use tokio_util::compat::{Compat as TokioCompat, FuturesAsyncReadCompatExt};
use wasm_streams::readable::{IntoAsyncRead as ReadableStreamReader, ReadableStream};
use web_sys::{Blob, FileReaderSync};

use super::error::WasmError;

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
                    .map_err(WasmError::from)?,
            );
        }
        self.offset = offset;
        Ok(offset)
    }
}

pub struct AsyncBlobReader {
    blob: SeekableBlob,
    reader: TokioCompat<ReadableStreamReader<'static>>,
    bytes_read: usize,
}

impl AsyncBlobReader {
    pub fn new(blob: Blob) -> Self {
        Self {
            reader: ReadableStream::from_raw(blob.stream())
                .into_async_read()
                .compat(),
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
        self.reader = ReadableStream::from_raw(self.blob.blob().stream())
            .into_async_read()
            .compat();
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
                self.bytes_read += buf.filled().len() - already_filled;
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
        self.bytes_read += size;
        Ok(size)
    }
}
