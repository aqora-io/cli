use std::fmt;

use thiserror::Error;

#[cfg(feature = "threaded")]
pub(crate) type DynError = dyn std::error::Error + Send + Sync + 'static;
#[cfg(not(feature = "threaded"))]
pub(crate) type DynError = dyn std::error::Error + 'static;

pub type BoxError = Box<DynError>;

#[derive(Debug)]
pub enum MiddlewareError {
    Request(reqwest::Error),
    Middleware(BoxError),
}

impl fmt::Display for MiddlewareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(err) => err.fmt(f),
            Self::Middleware(err) => err.fmt(f),
        }
    }
}

impl<T> From<T> for MiddlewareError
where
    T: Into<BoxError>,
{
    fn from(error: T) -> Self {
        Self::Middleware(error.into())
    }
}

#[cfg(feature = "ws")]
#[derive(Debug)]
pub enum WsMiddlewareError {
    #[cfg(feature = "tokio-ws")]
    Request(tokio_tungstenite::tungstenite::error::Error),
    Middleware(BoxError),
}

#[cfg(feature = "ws")]
impl<T> From<T> for WsMiddlewareError
where
    T: Into<BoxError>,
{
    fn from(error: T) -> Self {
        Self::Middleware(error.into())
    }
}

#[cfg(feature = "ws")]
impl fmt::Display for WsMiddlewareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "tokio-ws")]
            Self::Request(err) => err.fmt(f),
            Self::Middleware(err) => err.fmt(f),
        }
    }
}

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("Invalid ETag")]
    InvalidETag,
    #[error("Invalid Content-Length")]
    InvalidContentLength,
    #[error("Invalid Content-Type")]
    InvalidContentType,
    #[error("Invalid Content-Disposition")]
    InvalidContentDisposition,
    #[error("Invalid Content-Disposition")]
    MissingBody,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("GraphQL response contained errors: {0:?}")]
    Response(Vec<graphql_client::Error>),
    #[error(transparent)]
    S3(#[from] S3Error),
    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
    #[cfg(feature = "tokio-ws")]
    #[error(transparent)]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::error::Error),
    #[cfg(feature = "ws")]
    #[error(transparent)]
    GraphQLWs(#[from] graphql_ws_client::Error),
    #[cfg(feature = "ws")]
    #[error("Websocket closed")]
    WsClosed,
    #[error("GraphQL response contained no data")]
    NoData,
    #[error(transparent)]
    Middleware(BoxError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<MiddlewareError> for Error {
    fn from(error: MiddlewareError) -> Self {
        match error {
            MiddlewareError::Request(err) => Self::Request(err),
            MiddlewareError::Middleware(err) => Self::Middleware(err),
        }
    }
}

#[cfg(feature = "ws")]
impl From<WsMiddlewareError> for Error {
    fn from(error: WsMiddlewareError) -> Self {
        match error {
            #[cfg(feature = "tokio-ws")]
            WsMiddlewareError::Request(err) => Self::Tungstenite(err),
            WsMiddlewareError::Middleware(err) => Self::Middleware(err),
        }
    }
}
