use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use std::path::Path;
use url::Url;

pub mod custom_scalars {
    pub type Semver = String;
}

pub use aqora_client::{client::send, CredentialsProvider, Error as GraphQLError};

pub type GraphQLClient = aqora_client::Client<Option<Credentials>>;

impl From<GraphQLError> for Error {
    fn from(error: GraphQLError) -> Self {
        match error {
            GraphQLError::Request(error) => {
                error::system(&format!("Request failed: {error:?}"), "")
            }
            GraphQLError::Json(error) => {
                error::system(&format!("Failed to parse JSON: {error:?}"), "")
            }
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
            GraphQLError::NoData => error::system("Invalid response received from server", ""),
            GraphQLError::Credentials(error) => {
                error::system(&format!("Invalid credentials: {error:?}"), "")
            }
            GraphQLError::InvalidHeaderValue(_) => {
                error::system("Invalid header value from client", "")
            }
        }
    }
}

#[async_trait::async_trait]
impl CredentialsProvider for Credentials {
    type Error = Error;
    async fn access_token(&self, url: &Url) -> Result<Option<String>> {
        if let Some(credentials) = self.clone().refresh(url).await? {
            return Ok(Some(credentials.access_token));
        }
        Ok(None)
    }
}

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

const AQORA_USER_AGENT: HeaderValue = HeaderValue::from_static(concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION")
));

pub async fn new(config_home: impl AsRef<Path>, url: Url) -> Result<GraphQLClient> {
    let credentials = get_credentials(config_home, url.clone()).await?;
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, AQORA_USER_AGENT);
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    Ok(GraphQLClient::new_with_client(
        client,
        graphql_url(&url)?,
        credentials,
    ))
}
