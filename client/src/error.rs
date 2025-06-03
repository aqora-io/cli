use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("GraphQL response contained errors: {0:?}")]
    Response(Vec<graphql_client::Error>),
    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
    #[cfg(feature = "ws")]
    #[error(transparent)]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::error::Error),
    #[cfg(feature = "ws")]
    #[error(transparent)]
    GraphQLWs(#[from] graphql_ws_client::Error),
    #[error("GraphQL response contained no data")]
    NoData,
    #[error(transparent)]
    Credentials(Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
