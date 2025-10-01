use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
use aqora_client::{
    credentials::{CredentialsLayer, CredentialsProvider},
    error::BoxError,
    trace::TraceLayer,
    ClientOptions,
};
use axum::http::Uri;
use reqwest::header::{HeaderValue, ORIGIN, USER_AGENT};
use std::path::Path;
use tokio::sync::Mutex;
use url::Url;

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
            GraphQLError::BadOrigin(err) => error::user(
                &format!("Bad origin requested {err}"),
                "Try a different aqora URL",
            ),
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

struct CredentialsForClient {
    credentials: Mutex<Option<Credentials>>,
    unauthenticated_client: aqora_client::Client,
}

impl CredentialsForClient {
    async fn load(
        config_home: impl AsRef<Path>,
        unauthenticated_client: aqora_client::Client,
    ) -> Result<Self> {
        let credentials = get_credentials(config_home, unauthenticated_client.clone()).await?;
        Ok(Self {
            credentials: Mutex::new(credentials),
            unauthenticated_client,
        })
    }
}

#[async_trait::async_trait]
impl CredentialsProvider for CredentialsForClient {
    fn authenticates(&self, uri: &Uri) -> Result<bool, BoxError> {
        Ok(aqora_client::utils::host_matches(
            self.unauthenticated_client.url(),
            &Url::parse(&uri.to_string())?,
        )?)
    }

    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        let mut creds = self.credentials.lock().await;
        if let Some(credentials) = creds.take() {
            *creds = credentials.refresh(&self.unauthenticated_client).await?;
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

fn origin(url: &Url) -> Result<HeaderValue> {
    let mut origin = format!(
        "{}://{}",
        url.scheme(),
        url.host_str().ok_or_else(|| {
            error::user(
                &format!("Invalid url {url}: no host"),
                "Try a different AQORA_URL",
            )
        })?
    );
    if let Some(port) = url.port() {
        origin.push_str(&format!(":{}", port));
    }
    Ok(origin.parse()?)
}

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

pub fn unauthenticated_client(url: Url, options: ClientOptions) -> Result<GraphQLClient> {
    if options.allow_insecure_host && !aqora_client::utils::is_url_secure(&url)? {
        tracing::warn!("Using insecure host: {url}");
    }
    let mut client = GraphQLClient::new_with_options(graphql_url(&url)?, options);
    client
        .graphql_layer(TraceLayer::new().debug_body(true))
        .graphql_layer(tower_http::set_header::SetRequestHeaderLayer::appending(
            USER_AGENT,
            AQORA_USER_AGENT,
        ))
        .graphql_layer(tower_http::set_header::SetRequestHeaderLayer::appending(
            ORIGIN,
            origin(&url)?,
        ))
        .s3_layer(TraceLayer::new())
        .ws_layer(TraceLayer::new());
    Ok(client)
}

pub async fn authenticate_client(
    config_home: impl AsRef<Path>,
    mut client: aqora_client::Client,
) -> Result<GraphQLClient> {
    let creds =
        CredentialsLayer::new(CredentialsForClient::load(config_home, client.clone()).await?);
    client.graphql_layer(creds.clone());
    client.s3_layer(creds);
    Ok(client)
}
