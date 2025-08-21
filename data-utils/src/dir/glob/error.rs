use thiserror::Error;

#[derive(Error, Debug)]
pub enum GlobError {
    #[error("Unexpected path component: {0}")]
    UnexpectedPathComponent(String),
    #[error(transparent)]
    Parse(#[from] nom::Err<(String, nom::error::ErrorKind)>),
    #[error("Path '{0}' is not valid Utf8")]
    InvalidUtf8(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    StripPrefix(#[from] std::path::StripPrefixError),
    #[error(transparent)]
    Regex(#[from] ::regex::Error),
}
