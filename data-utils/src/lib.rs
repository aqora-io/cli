#[cfg(feature = "csv")]
pub mod csv;
#[cfg(feature = "json")]
pub mod json;
pub mod schema;
mod utils;
#[cfg(feature = "wasm")]
pub mod wasm;

use arrow::datatypes::Schema;
use thiserror::Error;
use tokio::io::{self, AsyncRead, AsyncSeek};

#[derive(Debug, Clone)]
pub enum ReadOptions {
    #[cfg(feature = "parquet")]
    Parquet,
    #[cfg(feature = "csv")]
    Csv(csv::Format),
    #[cfg(feature = "json")]
    Json(json::Format),
    #[cfg(feature = "ipc")]
    Ipc,
}

#[derive(Debug, Error)]
pub enum InferError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[cfg(feature = "parquet")]
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
}

type InferResult<T, E = InferError> = std::result::Result<T, E>;

pub async fn infer_schema<R>(
    reader: R,
    options: &ReadOptions,
    max_records: Option<usize>,
) -> InferResult<Schema>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    unimplemented!()
}
