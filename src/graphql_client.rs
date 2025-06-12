use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
use aqora_client::{
    checksum::{crc32fast::Crc32, S3ChecksumMiddleware},
    credentials::{CredentialsMiddleware, CredentialsProvider},
    middleware::{Middleware, MiddlewareError, Next},
    trace::DebugMiddleware,
};
use futures::future::{BoxFuture, FutureExt};
use reqwest::header::{HeaderValue, USER_AGENT};
use std::path::Path;
use url::Url;

pub mod custom_scalars {
    pub type Semver = String;
}

pub use aqora_client::{Client as GraphQLClient, Error as GraphQLError};

impl From<GraphQLError> for Error {
    fn from(error: GraphQLError) -> Self {
        match error {
            GraphQLError::Request(error) => {
                error::system(&format!("Request failed: {error:?}"), "")
            }
            GraphQLError::Json(error) => {
                error::system(&format!("Failed to parse JSON: {error:?}"), "")
            }
            GraphQLError::S3(error) => error::system(&format!("S3 Error: {error:?}"), ""),
            GraphQLError::Response(errors) => error::user(
                &errors
                    .into_iter()
                    .map(|error| error.message)
                    .filter(|error| !error.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join("\n"),
                "Check your arguments and try again",
            ),
            GraphQLError::Tungstenite(error) => {
                error::system(&format!("Websocket failed: {error:?}"), "")
            }
            GraphQLError::GraphQLWs(error) => {
                error::system(&format!("Subscription failed: {error:?}"), "")
            }
            GraphQLError::WsClosed => error::system("Websocket closed early", ""),
            GraphQLError::NoData => error::system("Invalid response received from server", ""),
            GraphQLError::Middleware(error) => {
                error::system(&format!("Middleware error: {error:?}"), "")
            }
            GraphQLError::InvalidHeaderValue(_) => {
                error::system("Invalid header value from client", "")
            }
        }
    }
}

impl CredentialsProvider for Credentials {
    fn access_token<'a>(
        &'a self,
        url: &Url,
    ) -> BoxFuture<'a, Result<Option<String>, Box<dyn std::error::Error + Send + Sync + 'static>>>
    {
        let url = url.clone();
        async move {
            if let Some(credentials) = self.clone().refresh(&url).await? {
                return Ok(Some(credentials.access_token));
            }
            Ok(None)
        }
        .boxed()
    }
}

const AQORA_USER_AGENT: HeaderValue = HeaderValue::from_static(concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION")
));

pub struct AqoraUserAgentMiddleware;

impl Middleware for AqoraUserAgentMiddleware {
    fn handle<'a>(
        &'a self,
        mut request: reqwest::Request,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<reqwest::Response, MiddlewareError>> {
        request.headers_mut().insert(USER_AGENT, AQORA_USER_AGENT);
        next.handle(request)
    }
}

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

pub fn unauthenticated_client(url: Url) -> Result<GraphQLClient> {
    let mut client = GraphQLClient::new(graphql_url(&url)?);
    client
        .with(AqoraUserAgentMiddleware)
        .with(DebugMiddleware)
        .s3_with(S3ChecksumMiddleware::new(Crc32::new()))
        .s3_with(DebugMiddleware)
        .ws_with(DebugMiddleware);
    Ok(client)
}

pub async fn client(config_home: impl AsRef<Path>, url: Url) -> Result<GraphQLClient> {
    let mut client = unauthenticated_client(url.clone())?;
    client.with(CredentialsMiddleware::new(
        get_credentials(config_home, url).await?,
    ));
    Ok(client)
}
