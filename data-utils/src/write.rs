use std::collections::VecDeque;
use std::io;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use bytes::Bytes;
use futures::{future::LocalBoxFuture, prelude::*};
use parquet::arrow::async_writer::AsyncArrowWriter;
use pin_project::pin_project;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub use parquet::arrow::arrow_writer::ArrowWriterOptions as Options;
pub use parquet::arrow::async_writer::AsyncFileWriter;
pub use parquet::format::FileMetaData;

use crate::error::{Error, Result};
use crate::read::RecordBatchStream;
use crate::schema::Schema;

pub struct AsyncWriteToFileWriter<T>(pub T);

impl<T> AsyncFileWriter for AsyncWriteToFileWriter<T>
where
    T: AsyncWrite + Unpin,
{
    fn write(&mut self, bs: Bytes) -> LocalBoxFuture<parquet::errors::Result<()>> {
        async move {
            self.0.write_all(&bs).await?;
            Ok(())
        }
        .boxed_local()
    }
    fn complete(&mut self) -> LocalBoxFuture<parquet::errors::Result<()>> {
        async move {
            self.0.flush().await?;
            self.0.shutdown().await?;
            Ok(())
        }
        .boxed_local()
    }
}

#[async_trait::async_trait(?Send)]
pub trait AsyncPartitionWriter {
    type Writer: AsyncFileWriter;
    async fn next_partition(&mut self) -> io::Result<Self::Writer>;
    fn max_partition_size(&self) -> Option<usize>;
}

pub struct SinglePart<W>(Option<W>);

impl<W> SinglePart<W> {
    pub fn new(writer: W) -> Self {
        Self(Some(writer))
    }
}

#[async_trait::async_trait(?Send)]
impl<W> AsyncPartitionWriter for SinglePart<W>
where
    W: AsyncFileWriter,
{
    type Writer = W;
    async fn next_partition(&mut self) -> io::Result<Self::Writer> {
        Ok(self.0.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "SinglePart writer already consumed",
            )
        })?)
    }
    fn max_partition_size(&self) -> Option<usize> {
        None
    }
}

#[async_trait::async_trait(?Send)]
impl<T> AsyncPartitionWriter for &mut T
where
    T: AsyncPartitionWriter,
{
    type Writer = T::Writer;
    async fn next_partition(&mut self) -> io::Result<Self::Writer> {
        T::next_partition(self).await
    }

    fn max_partition_size(&self) -> Option<usize> {
        T::max_partition_size(self)
    }
}

async fn close_part<W>(
    mut part_writer: AsyncArrowWriter<W::Writer>,
) -> Result<(W::Writer, FileMetaData)>
where
    W: AsyncPartitionWriter,
{
    let meta = part_writer.finish().await?;
    let writer = part_writer.into_inner();
    Ok((writer, meta))
}

type ClosePartFut<'w, Writer> = LocalBoxFuture<'w, Result<(Writer, FileMetaData)>>;

async fn create_part<W>(
    mut writer: W,
    schema: SchemaRef,
    options: Options,
) -> Result<(W, AsyncArrowWriter<W::Writer>)>
where
    W: AsyncPartitionWriter,
{
    let part_writer = AsyncArrowWriter::try_new_with_options(
        writer.next_partition().await?,
        schema.clone(),
        options.clone(),
    )?;
    Ok((writer, part_writer))
}

async fn write_part<W>(
    writer: W,
    mut part_writer: AsyncArrowWriter<W::Writer>,
    record_batch: RecordBatch,
) -> Result<(W, AsyncArrowWriter<W::Writer>)>
where
    W: AsyncPartitionWriter,
{
    part_writer.write(&record_batch).await?;
    Ok((writer, part_writer))
}

type PartWriterFut<'w, W, Writer> = LocalBoxFuture<'w, Result<(W, AsyncArrowWriter<Writer>)>>;

async fn owned_next<S, T>(mut stream: S) -> Option<(S, T)>
where
    S: Stream<Item = T> + Unpin,
{
    stream.next().await.map(|next| (stream, next))
}

type OwnedNextFut<'s, S, T> = LocalBoxFuture<'s, Option<(S, T)>>;

enum WriteState<'w, W>
where
    W: AsyncPartitionWriter + 'w,
{
    Waiting(Option<(W, AsyncArrowWriter<W::Writer>)>),
    Busy(PartWriterFut<'w, W, W::Writer>),
}

#[pin_project]
pub struct ParquetStream<'s, 'w, S, W>
where
    S: Stream<Item = Result<RecordBatch, Error>> + Unpin + 's,
    W: AsyncPartitionWriter + 'w,
{
    schema: SchemaRef,
    options: Options,
    max_part_size: Option<usize>,
    batch_buffer: VecDeque<RecordBatch>,
    batch_buffer_size: Option<usize>,
    read_fut: Option<OwnedNextFut<'s, S, Result<RecordBatch>>>,
    write_state: WriteState<'w, W>,
    closing_futs: Vec<ClosePartFut<'w, W::Writer>>,
}

impl<S, W> ParquetStream<'_, '_, S, W>
where
    S: Stream<Item = Result<RecordBatch, Error>> + Unpin,
    W: AsyncPartitionWriter,
{
    pub fn new(
        stream: S,
        writer: W,
        schema: Schema,
        options: Options,
        batch_buffer_size: Option<usize>,
    ) -> Self {
        let max_part_size = writer.max_partition_size();
        let schema: SchemaRef = Arc::new(schema.into());
        let write_state =
            WriteState::Busy(create_part(writer, schema.clone(), options.clone()).boxed_local());
        let batch_buffer = VecDeque::new();
        let read_fut = Some(owned_next(stream).boxed_local());
        Self {
            schema,
            options,
            max_part_size,
            batch_buffer,
            batch_buffer_size,
            read_fut,
            write_state,
            closing_futs: Vec::new(),
        }
    }
}

impl<S, W> Stream for ParquetStream<'_, '_, S, W>
where
    S: Stream<Item = Result<RecordBatch, Error>> + Unpin,
    W: AsyncPartitionWriter,
{
    type Item = Result<(W::Writer, FileMetaData), Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            let close_pending = !this.closing_futs.is_empty();
            for (i, closing_fut) in this.closing_futs.iter_mut().enumerate() {
                match closing_fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((writer, meta))) => {
                        drop(this.closing_futs.remove(i));
                        return Poll::Ready(Some(Ok((writer, meta))));
                    }
                    Poll::Ready(Err(err)) => {
                        drop(this.closing_futs.remove(i));
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Pending => {}
                }
            }
            let read_pending = if this
                .batch_buffer_size
                .is_none_or(|size| this.batch_buffer.len() < size)
            {
                match this.read_fut.as_mut() {
                    Some(fut) => match fut.as_mut().poll(cx) {
                        Poll::Ready(Some((stream, next))) => {
                            *this.read_fut = Some(owned_next(stream).boxed_local());
                            match next {
                                Ok(record_batch) => this.batch_buffer.push_back(record_batch),
                                Err(err) => {
                                    return Poll::Ready(Some(Err(err)));
                                }
                            }
                            false
                        }
                        Poll::Ready(None) => {
                            *this.read_fut = None;
                            false
                        }
                        Poll::Pending => true,
                    },
                    None => false,
                }
            } else {
                false
            };
            let write_pending = match this.write_state {
                WriteState::Waiting(writers) => {
                    if !this.batch_buffer.is_empty() {
                        let (writer, part_writer) =
                            writers.take().expect("State should change after taken");
                        if this
                            .max_part_size
                            .is_some_and(|part_size| part_writer.bytes_written() >= part_size)
                        {
                            this.closing_futs
                                .push(close_part::<W>(part_writer).boxed_local());
                            *this.write_state = WriteState::Busy(
                                create_part(writer, this.schema.clone(), this.options.clone())
                                    .boxed_local(),
                            );
                        } else {
                            let record_batch = this
                                .batch_buffer
                                .pop_front()
                                .expect("Batch buffer should be checked to not be empty above");
                            *this.write_state = WriteState::Busy(
                                write_part(writer, part_writer, record_batch).boxed_local(),
                            )
                        }
                    }
                    false
                }
                WriteState::Busy(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((writer, part_writer))) => {
                        *this.write_state = WriteState::Waiting(Some((writer, part_writer)));
                        false
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                    Poll::Pending => true,
                },
            };

            let read_finished = this.read_fut.is_none();
            let write_finished =
                matches!(this.write_state, WriteState::Waiting(_)) && this.batch_buffer.is_empty();
            let close_finished = this.closing_futs.is_empty();

            if read_finished
                && !read_pending
                && write_finished
                && !write_pending
                && close_finished
                && !close_pending
            {
                return Poll::Ready(None);
            } else if (read_finished || read_pending)
                && (write_finished || write_pending)
                && (close_finished || close_pending)
            {
                return Poll::Pending;
            }
        }
    }
}

impl<'s, S, T, E> RecordBatchStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin + 's,
    Error: From<E>,
    T: serde::Serialize,
{
    pub fn write_to_parquet<'w, W>(
        self,
        writer: W,
        options: Options,
        batch_buffer_size: Option<usize>,
    ) -> ParquetStream<'s, 'w, Self, W>
    where
        W: AsyncPartitionWriter + 'w,
    {
        let schema = self.schema().clone();
        ParquetStream::new(self, writer, schema, options, batch_buffer_size)
    }
}
