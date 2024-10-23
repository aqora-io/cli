#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Ignore(#[from] ignore::Error),
    #[error(transparent)]
    StripPrefix(#[from] std::path::StripPrefixError),
    #[error("unsupported compression")]
    UnsupportedCompression,

    #[cfg(feature = "tokio")]
    #[error(transparent)]
    Tokio(#[from] tokio::task::JoinError),
}

impl From<zip::result::ZipError> for Error {
    fn from(value: zip::result::ZipError) -> Self {
        Self::Io(value.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
