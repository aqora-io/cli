use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
use aqora_client::{
    credentials::{CredentialsLayer, CredentialsProvider},
    error::BoxError,
    trace::TraceLayer,
};
use axum::http::Uri;
use reqwest::header::{HeaderValue, USER_AGENT};
use std::path::Path;
use tokio::sync::Mutex;
use url::{Host, Url};

pub mod custom_scalars {
    pub type Semver = String;
    pub type UsernameOrID = String;
    pub type DateTime = chrono::DateTime<chrono::Utc>;
}

pub use aqora_client::{Client as GraphQLClient, Error as GraphQLError};

impl From<GraphQLError> for Error {
    fn from(error: GraphQLError) -> Self {
        match error {
            GraphQLError::Request(error) => {
                error::system(&format!("Request failed: {error:?}"), "")
            }
            GraphQLError::RequestBuilder(error) => {
                error::system(&format!("Couldn't build request: {error:?}"), "")
            }
            GraphQLError::BadStatus(status) => error::system(
                &format!("Received an invalid response code from server: {status}"),
                "",
            ),
            GraphQLError::StreamNotSupported => {
                error::system("Streaming is not supported by this client", "")
            }
            GraphQLError::Json(error) => {
                error::system(&format!("Failed to parse JSON: {error:?}"), "")
            }
            GraphQLError::S3(error) => error::system(&format!("S3 Error: {error:?}"), ""),
            GraphQLError::BadS3Range => error::system("Bad S3 range requested", ""),
            GraphQLError::BadOrigin => error::system("Bad origin requested", ""),
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
            GraphQLError::InvalidHeaderValue(_) => {
                error::system("Invalid header value from client", "")
            }
            GraphQLError::Middleware(error) => {
                error::system(&format!("Request error: {error:?}"), "")
            }
        }
    }
}

struct CredentialsForUrl {
    credentials: Mutex<Option<Credentials>>,
    url: Url,
}

impl CredentialsForUrl {
    async fn load(config_home: impl AsRef<Path>, url: Url) -> Result<Self> {
        let credentials = get_credentials(config_home, url.clone()).await?;
        Ok(Self {
            credentials: Mutex::new(credentials),
            url,
        })
    }
}

#[async_trait::async_trait]
impl CredentialsProvider for CredentialsForUrl {
    fn authenticates(&self, url: &Uri) -> bool {
        let Some(scheme) = url.scheme_str() else {
            return false;
        };
        aqora_client::allow_request_url(
            scheme,
            url.host().and_then(|host| Host::parse(host).ok()),
            self.url.host(),
        )
    }

    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        let mut creds = self.credentials.lock().await;
        if let Some(credentials) = creds.take() {
            *creds = credentials.refresh(&self.url).await?;
        }
        if let Some(credentials) = creds.as_ref() {
            return Ok(Some(credentials.access_token.clone()));
        }
        Ok(None)
    }
}

const AQORA_USER_AGENT: HeaderValue = HeaderValue::from_static(concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION")
));

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

pub fn unauthenticated_client(url: Url) -> Result<GraphQLClient> {
    let mut client = GraphQLClient::new(graphql_url(&url)?);
    client
        .graphql_layer(TraceLayer::new().debug_body(true))
        .graphql_layer(tower_http::set_header::SetRequestHeaderLayer::appending(
            USER_AGENT,
            AQORA_USER_AGENT,
        ))
        .s3_layer(TraceLayer::new())
        .ws_layer(TraceLayer::new());
    Ok(client)
}

pub async fn client(config_home: impl AsRef<Path>, url: Url) -> Result<GraphQLClient> {
    let mut client = unauthenticated_client(url.clone())?;
    let creds = CredentialsLayer::new(CredentialsForUrl::load(config_home, url).await?);
    client.graphql_layer(creds.clone());
    client.s3_layer(creds);
    Ok(client)
}
