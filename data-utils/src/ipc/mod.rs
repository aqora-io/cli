use std::pin::Pin;
use std::task::{ready, Context, Poll};

use arrow::buffer::Buffer as ArrowBuffer;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamDecoder;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions as BaseIpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use bytes::{buf::Writer as BytesWriter, BufMut, Bytes, BytesMut};
use futures::prelude::*;
use pin_project::pin_project;
use tokio::io::{self, AsyncRead};

use crate::error::Result;
use crate::process::{ByteProcessResult, ByteProcessor, ProcessItem, ProcessReadStream};
use crate::read::{RecordBatchStream, RecordBatchStreamExt};
use crate::schema::Schema;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub struct IpcWriteOptions {
    alignment: usize,
    write_legacy_ipc_format: bool,
    metadata_version: arrow::ipc::MetadataVersion,
    compression: Option<arrow::ipc::CompressionType>,
    buffer_capacity: usize,
}

impl Default for IpcWriteOptions {
    fn default() -> Self {
        Self {
            alignment: 64,
            write_legacy_ipc_format: false,
            metadata_version: arrow::ipc::MetadataVersion::V5,
            compression: None,
            buffer_capacity: DEFAULT_BUF_SIZE,
        }
    }
}

impl TryFrom<IpcWriteOptions> for BaseIpcWriteOptions {
    type Error = ArrowError;
    fn try_from(value: IpcWriteOptions) -> Result<Self, Self::Error> {
        let mut options = BaseIpcWriteOptions::try_new(
            value.alignment,
            value.write_legacy_ipc_format,
            value.metadata_version,
        )?;
        options = options.try_with_compression(value.compression)?;
        Ok(options)
    }
}

struct IpcBufWriter<W> {
    buffer: BytesWriter<BytesMut>,
    buffer_capacity: usize,
    writer: W,
}

impl<W> IpcBufWriter<W> {
    pub fn new(writer: W, buffer_capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(buffer_capacity).writer(),
            buffer_capacity,
            writer,
        }
    }
}

impl<W> IpcBufWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn do_write(&mut self) -> io::Result<()> {
        let bytes = self.buffer.get_mut();
        while bytes.len() > self.buffer_capacity {
            let written = self.writer.write(bytes).await?;
            let _ = bytes.split_to(written);
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        let bytes = self.buffer.get_mut();
        self.writer.write_all(bytes).await?;
        bytes.clear();
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        self.flush().await?;
        self.writer.close().await?;
        Ok(())
    }
}

impl<W> std::io::Write for IpcBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        // flush handled asynchronously
        Ok(())
    }
}

pub struct IpcStreamWriter<W> {
    inner: StreamWriter<IpcBufWriter<W>>,
}

impl<W> IpcStreamWriter<W> {
    pub fn try_new(
        writer: W,
        schema: Schema,
        options: IpcWriteOptions,
    ) -> Result<Self, ArrowError> {
        let inner = StreamWriter::try_new_with_options(
            IpcBufWriter::new(writer, options.buffer_capacity),
            &schema.into(),
            options.try_into()?,
        )?;
        Ok(Self { inner })
    }
}

impl<W: AsyncWrite + Unpin> IpcStreamWriter<W> {
    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<(), ArrowError> {
        self.inner.write(record_batch)?;
        self.inner.get_mut().do_write().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ArrowError> {
        self.inner.flush()?;
        self.inner.get_mut().flush().await?;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), ArrowError> {
        self.inner.finish()?;
        self.inner.get_mut().finish().await?;
        Ok(())
    }
}

pub struct IpcFileWriter<W> {
    inner: FileWriter<IpcBufWriter<W>>,
}

impl<W> IpcFileWriter<W> {
    pub fn try_new(
        writer: W,
        schema: Schema,
        options: IpcWriteOptions,
    ) -> Result<Self, ArrowError> {
        let inner = FileWriter::try_new_with_options(
            IpcBufWriter::new(writer, options.buffer_capacity),
            &schema.into(),
            options.try_into()?,
        )?;
        Ok(Self { inner })
    }
}

impl<W: AsyncWrite + Unpin> IpcFileWriter<W> {
    pub fn write_metadata(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), ArrowError> {
        self.inner.write_metadata(key, value);
        Ok(())
    }

    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<(), ArrowError> {
        self.inner.write(record_batch)?;
        self.inner.get_mut().do_write().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ArrowError> {
        self.inner.flush()?;
        self.inner.get_mut().flush().await?;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), ArrowError> {
        self.inner.finish()?;
        self.inner.get_mut().finish().await?;
        Ok(())
    }
}

pub async fn write<W, S>(writer: W, record_batches: S, options: IpcWriteOptions) -> Result<()>
where
    W: AsyncWrite + Unpin,
    S: RecordBatchStream + Unpin,
{
    let mut stream_writer = IpcStreamWriter::try_new(writer, record_batches.schema(), options)?;
    let mut stream = record_batches.wrap();
    while let Some(record_batch) = stream.try_next().await? {
        stream_writer.write(&record_batch).await?;
    }
    stream_writer.finish().await?;
    Ok(())
}

#[pin_project]
pub struct IpcReader<R> {
    schema: Schema,
    #[pin]
    stream: ProcessReadStream<R, IpcProcessor>,
}

pub async fn read<R>(reader: R, require_alignment: bool) -> io::Result<IpcReader<R>>
where
    R: AsyncRead + Unpin,
{
    let processor = IpcProcessor::new(require_alignment);
    let mut stream = ProcessReadStream::new(reader, processor);
    let next = stream.try_next().await?;
    let schema = match next {
        Some(ProcessItem {
            item: IpcItem::Schema(schema),
            ..
        }) => schema.into(),
        _ => {
            return Err(io::Error::other(
                "Expected IPC schema at the start of the stream",
            ));
        }
    };
    Ok(IpcReader { schema, stream })
}

impl<R> RecordBatchStream for IpcReader<R>
where
    R: AsyncRead,
{
    fn schema(&self) -> Schema {
        self.schema.clone()
    }
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        match ready!(self.project().stream.poll_next(cx)) {
            Some(Ok(ProcessItem {
                item: IpcItem::RecordBatch(record_batch),
                ..
            })) => Poll::Ready(Some(Ok(record_batch))),
            Some(Ok(ProcessItem {
                item: IpcItem::Schema(_),
                ..
            })) => Poll::Ready(Some(Err(io::Error::other(
                "Expected IPC record batch but received schema",
            )
            .into()))),
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

struct IoCompatArrowError(ArrowError);

impl From<ArrowError> for IoCompatArrowError {
    fn from(value: ArrowError) -> Self {
        Self(value)
    }
}

impl From<IoCompatArrowError> for std::io::Error {
    fn from(value: IoCompatArrowError) -> Self {
        match value.0 {
            ArrowError::IoError(_, err) => err,
            err => std::io::Error::other(err),
        }
    }
}

enum IpcItem {
    Schema(SchemaRef),
    RecordBatch(RecordBatch),
}

#[derive(Default)]
struct IpcProcessor {
    schema_read: bool,
    next: Option<RecordBatch>,
    decoder: StreamDecoder,
}

impl IpcProcessor {
    pub fn new(require_alignment: bool) -> Self {
        Self {
            schema_read: false,
            next: None,
            decoder: StreamDecoder::new().with_require_alignment(require_alignment),
        }
    }

    pub fn decode(&mut self, bytes: Bytes, is_eof: bool) -> Result<usize, IoCompatArrowError> {
        let mut buffer = ArrowBuffer::from(bytes);
        match self.decoder.decode(&mut buffer)? {
            Some(next) => {
                self.next = Some(next);
            }
            None => {
                if is_eof {
                    self.decoder.finish()?;
                }
            }
        }
        Ok(buffer.ptr_offset())
    }

    pub fn take_next(&mut self) -> Option<IpcItem> {
        if self.schema_read {
            self.next.take().map(IpcItem::RecordBatch)
        } else if let Some(schema) = self.decoder.schema() {
            self.schema_read = true;
            Some(IpcItem::Schema(schema))
        } else {
            None
        }
    }
}

impl ByteProcessor for IpcProcessor {
    type Item = IpcItem;
    type Error = IoCompatArrowError;
    fn process(
        &mut self,
        bytes: Bytes,
        is_eof: bool,
    ) -> ByteProcessResult<Self::Item, Self::Error> {
        if let Some(next) = self.take_next() {
            return ByteProcessResult::Ok((0, 0, next));
        }
        match self.decode(bytes, is_eof) {
            Ok(consumed) => {
                if let Some(next) = self.take_next() {
                    ByteProcessResult::Ok((0, consumed, next))
                } else if is_eof {
                    ByteProcessResult::Done(consumed)
                } else {
                    ByteProcessResult::NotReady(consumed)
                }
            }
            Err(err) => ByteProcessResult::Err(err),
        }
    }
}
