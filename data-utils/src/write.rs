use std::io;
use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::prelude::*;
use parquet::arrow::async_writer::AsyncArrowWriter;
use tokio::io::AsyncWrite;

pub use parquet::arrow::arrow_writer::ArrowWriterOptions as Options;
pub use parquet::arrow::async_writer::AsyncFileWriter;
pub use parquet::format::FileMetaData;

use crate::error::{Error, Result};
use crate::read::RecordBatchStream;
use crate::schema::Schema;

#[async_trait::async_trait]
pub trait AsyncPartWriter<'a> {
    type Writer: AsyncFileWriter + 'a;
    async fn create_part(&'a mut self, num: usize) -> io::Result<Self::Writer>;
    fn max_part_size(&self) -> Option<usize>;
}

pub struct SinglePart<W>(W);

impl<W> SinglePart<W> {
    pub fn new(writer: W) -> Self {
        Self(writer)
    }

    pub fn into_inner(self) -> W {
        self.0
    }
}

#[async_trait::async_trait]
impl<'a, W> AsyncPartWriter<'a> for SinglePart<W>
where
    W: AsyncWrite + Unpin + Send + 'a,
{
    type Writer = &'a mut W;
    async fn create_part(&'a mut self, _: usize) -> io::Result<Self::Writer> {
        Ok(&mut self.0)
    }
    fn max_part_size(&self) -> Option<usize> {
        None
    }
}

#[async_trait::async_trait]
impl<'a, T> AsyncPartWriter<'a> for &mut T
where
    T: AsyncPartWriter<'a> + Send + Sync,
{
    type Writer = T::Writer;
    async fn create_part(&'a mut self, num: usize) -> io::Result<Self::Writer> {
        T::create_part(self, num).await
    }

    fn max_part_size(&self) -> Option<usize> {
        T::max_part_size(self)
    }
}

pub async fn parquet_from_stream<S, W, E>(
    mut stream: S,
    mut writer: W,
    schema: Schema,
    options: Options,
) -> Result<Vec<FileMetaData>>
where
    S: Stream<Item = Result<RecordBatch, E>> + Unpin,
    Error: From<E>,
    W: for<'a> AsyncPartWriter<'a>,
{
    let mut part_num = 0;
    let max_part_size = writer.max_part_size();
    let schema: SchemaRef = Arc::new(schema.into());
    let mut part_writer = AsyncArrowWriter::try_new_with_options(
        writer.create_part(part_num).await?,
        schema.clone(),
        options.clone(),
    )?;
    let mut out = vec![];
    loop {
        if max_part_size.is_some_and(|part_size| part_writer.bytes_written() >= part_size) {
            out.push(part_writer.close().await?);
            part_num += 1;
            part_writer = AsyncArrowWriter::try_new_with_options(
                writer.create_part(part_num).await?,
                schema.clone(),
                options.clone(),
            )?;
        }
        if let Some(record_batch) = stream.next().await.transpose()? {
            part_writer.write(&record_batch).await?;
        } else {
            out.push(part_writer.close().await?);
            return Ok(out);
        }
    }
}

impl<S, T, E> RecordBatchStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    Error: From<E>,
    T: serde::Serialize,
{
    pub async fn write_to_parquet<W>(self, writer: W, options: Options) -> Result<Vec<FileMetaData>>
    where
        W: for<'a> AsyncPartWriter<'a>,
    {
        let schema = self.schema().clone();
        parquet_from_stream::<_, _, Error>(self, writer, schema, options).await
    }
}
