use std::collections::VecDeque;
use std::io;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use futures::prelude::*;
use parquet::arrow::async_writer::AsyncArrowWriter;
use pin_project::pin_project;

pub use parquet::arrow::arrow_writer::ArrowWriterOptions as Options;
pub use parquet::arrow::async_writer::AsyncFileWriter;
pub use parquet::format::FileMetaData;

use crate::async_util::parquet_async::*;
use crate::error::{Error, Result};
use crate::read::{RecordBatchStream, RecordBatchStreamExt, WrappedRecordBatchStream};
use crate::schema::Schema;

#[derive(Debug, Clone, Copy, Default)]
pub struct BufferOptions {
    pub batch_buffer_size: Option<usize>,
    pub row_group_size: Option<usize>,
    pub small_first_row_group: bool,
}

#[cfg_attr(feature = "parquet-no-send", async_trait(?Send))]
#[cfg_attr(not(feature = "parquet-no-send"), async_trait)]
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

#[cfg_attr(feature = "parquet-no-send", async_trait(?Send))]
#[cfg_attr(not(feature = "parquet-no-send"), async_trait)]
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

#[cfg_attr(feature = "parquet-no-send", async_trait(?Send))]
#[cfg_attr(not(feature = "parquet-no-send"), async_trait)]
impl<T> AsyncPartitionWriter for &mut T
where
    T: AsyncPartitionWriter + MaybeSend,
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

type ClosePartFut<'w, Writer> = BoxFuture<'w, Result<(Writer, FileMetaData)>>;

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
    force_flush: bool,
    row_group_size: Option<usize>,
) -> Result<(W, AsyncArrowWriter<W::Writer>)>
where
    W: AsyncPartitionWriter,
{
    part_writer.write(&record_batch).await?;
    if force_flush
        || row_group_size.is_some_and(|row_group_size| part_writer.memory_size() >= row_group_size)
    {
        part_writer.flush().await?;
    }
    Ok((writer, part_writer))
}

type PartWriterFut<'w, W, Writer> = BoxFuture<'w, Result<(W, AsyncArrowWriter<Writer>)>>;

async fn owned_next<S, T>(mut stream: S) -> Option<(S, T)>
where
    S: Stream<Item = T> + Unpin,
{
    stream.next().await.map(|next| (stream, next))
}

type OwnedNextFut<'s, S, T> = BoxFuture<'s, Option<(S, T)>>;

enum WriteState<'w, W>
where
    W: AsyncPartitionWriter + 'w,
{
    Waiting {
        empty: bool,
        writers: Box<Option<(W, AsyncArrowWriter<W::Writer>)>>,
    },
    Busy {
        empty: bool,
        fut: PartWriterFut<'w, W, W::Writer>,
    },
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
    buffer_options: BufferOptions,
    batch_buffer: VecDeque<RecordBatch>,
    read_fut: Option<OwnedNextFut<'s, S, Result<RecordBatch>>>,
    write_state: WriteState<'w, W>,
    closing_futs: Vec<ClosePartFut<'w, W::Writer>>,
    is_first_record_batch: bool,
}

impl<S, W> ParquetStream<'_, '_, S, W>
where
    S: Stream<Item = Result<RecordBatch, Error>> + MaybeSend + Unpin,
    W: AsyncPartitionWriter + MaybeSend,
{
    pub fn new(
        stream: S,
        writer: W,
        schema: Schema,
        options: Options,
        buffer_options: BufferOptions,
    ) -> Self {
        let max_part_size = writer.max_partition_size();
        let schema: SchemaRef = Arc::new(schema.into());
        let write_state = WriteState::Busy {
            empty: true,
            fut: boxed_fut(create_part(writer, schema.clone(), options.clone())),
        };
        let batch_buffer = VecDeque::new();
        let read_fut = Some(boxed_fut(owned_next(stream)));
        Self {
            schema,
            options,
            max_part_size,
            buffer_options,
            batch_buffer,
            read_fut,
            write_state,
            closing_futs: Vec::new(),
            is_first_record_batch: true,
        }
    }
}

impl<S, W> Stream for ParquetStream<'_, '_, S, W>
where
    S: Stream<Item = Result<RecordBatch, Error>> + MaybeSend + Unpin,
    W: AsyncPartitionWriter + MaybeSend,
{
    type Item = Result<(W::Writer, FileMetaData), Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            let read_pending = if this
                .buffer_options
                .batch_buffer_size
                .is_none_or(|size| this.batch_buffer.len() < size)
            {
                match this.read_fut.as_mut() {
                    Some(fut) => match fut.as_mut().poll(cx) {
                        Poll::Ready(Some((stream, next))) => {
                            *this.read_fut = Some(boxed_fut(owned_next(stream)));
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
            let read_finished = this.read_fut.is_none();

            let write_pending = match this.write_state {
                WriteState::Waiting { empty, writers } => {
                    if this.batch_buffer.is_empty() {
                        if read_finished && !*empty {
                            if let Some((_, part_writer)) = writers.take() {
                                *empty = true;
                                this.closing_futs
                                    .push(boxed_fut(close_part::<W>(part_writer)));
                            }
                        }
                    } else {
                        let (writer, part_writer) =
                            writers.take().expect("State should change after taken");
                        if this
                            .max_part_size
                            .is_some_and(|part_size| part_writer.bytes_written() >= part_size)
                        {
                            this.closing_futs
                                .push(boxed_fut(close_part::<W>(part_writer)));
                            *this.write_state = WriteState::Busy {
                                empty: true,
                                fut: boxed_fut(create_part(
                                    writer,
                                    this.schema.clone(),
                                    this.options.clone(),
                                )),
                            };
                        } else {
                            let record_batch = this
                                .batch_buffer
                                .pop_front()
                                .expect("Batch buffer should be checked to not be empty above");
                            let force_flush = if *this.is_first_record_batch {
                                *this.is_first_record_batch = false;
                                this.buffer_options.small_first_row_group
                            } else {
                                false
                            };
                            *this.write_state = WriteState::Busy {
                                empty: false,
                                fut: boxed_fut(write_part(
                                    writer,
                                    part_writer,
                                    record_batch,
                                    force_flush,
                                    this.buffer_options.row_group_size,
                                )),
                            }
                        }
                    }
                    false
                }
                WriteState::Busy { empty, fut } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((writer, part_writer))) => {
                        *this.write_state = WriteState::Waiting {
                            empty: *empty,
                            writers: Box::new(Some((writer, part_writer))),
                        };
                        false
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Pending => true,
                },
            };
            let write_finished = match this.write_state {
                WriteState::Waiting { empty, .. } => {
                    this.batch_buffer.is_empty() && (!read_finished || *empty)
                }
                _ => false,
            };

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
            let close_finished = this.closing_futs.is_empty();

            let read_full = this
                .buffer_options
                .batch_buffer_size
                .is_some_and(|size| this.batch_buffer.len() >= size);

            if read_finished && !read_pending && write_finished && !write_pending && close_finished
            {
                return Poll::Ready(None);
            } else if (read_finished || read_full || read_pending)
                && (write_finished || write_pending)
            {
                return Poll::Pending;
            }
        }
    }
}

pub trait RecordBatchStreamParquetExt<'s>:
    Sized + RecordBatchStream + MaybeSend + Unpin + 's
{
    fn write_to_parquet<'w, W>(
        self,
        writer: W,
        options: Options,
        buffer_options: BufferOptions,
    ) -> ParquetStream<'s, 'w, WrappedRecordBatchStream<Self>, W>
    where
        W: AsyncPartitionWriter + MaybeSend + 'w;
}

impl<'s, S> RecordBatchStreamParquetExt<'s> for S
where
    S: Sized + RecordBatchStream + MaybeSend + Unpin + 's,
{
    fn write_to_parquet<'w, W>(
        self,
        writer: W,
        options: Options,
        buffer_options: BufferOptions,
    ) -> ParquetStream<'s, 'w, WrappedRecordBatchStream<Self>, W>
    where
        W: AsyncPartitionWriter + MaybeSend + 'w,
    {
        let schema = self.schema();
        ParquetStream::new(self.wrap(), writer, schema, options, buffer_options)
    }
}

#[cfg(test)]
mod test {

    #[cfg(feature = "fs")]
    #[tokio::test]
    async fn test_basic_json() {
        use super::*;
        use crate::read::ValueStream;
        use futures::stream::TryStreamExt;

        let tempdir = tempfile::TempDir::with_prefix("aqora-data-utils").unwrap();
        let writer = crate::fs::DirWriter::new(tempdir.path());
        let mut parquets = crate::fs::open("./tests/data/files/json/basic.json")
            .await
            .unwrap()
            .into_value_stream()
            .await
            .unwrap()
            .into_inferred_record_batch_stream(
                Default::default(),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap()
            .write_to_parquet(writer, Default::default(), Default::default());
        let mut file_count = 0;
        while let Some((file, meta)) = parquets.try_next().await.unwrap() {
            file_count += 1;
            assert!(file.metadata().await.unwrap().len() > 0);
            assert!(meta.num_rows > 0);
        }
        assert_eq!(file_count, 1);
    }
}
