use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::prelude::*;
use parquet::arrow::async_writer::AsyncArrowWriter;

pub use parquet::arrow::arrow_writer::ArrowWriterOptions as Options;
pub use parquet::arrow::async_writer::AsyncFileWriter;
pub use parquet::format::FileMetaData;

use crate::error::{Error, Result};
use crate::infer::Schema;

pub async fn from_stream<S, W, E>(
    mut stream: S,
    writer: W,
    schema: Schema,
    options: Options,
) -> Result<FileMetaData>
where
    S: Stream<Item = Result<RecordBatch, E>> + Unpin,
    Error: From<E>,
    W: AsyncFileWriter,
{
    let mut writer =
        AsyncArrowWriter::try_new_with_options(writer, Arc::new(schema.into()), options)?;
    while let Some(record_batch) = stream.next().await.transpose()? {
        writer.write(&record_batch).await?;
    }
    Ok(writer.close().await?)
}
