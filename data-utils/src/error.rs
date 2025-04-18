use std::convert::Infallible;
use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    SerdeArrow(#[from] serde_arrow::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
}

impl From<Infallible> for Error {
    fn from(err: Infallible) -> Self {
        match err {}
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
