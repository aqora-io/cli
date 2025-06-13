use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
use futures::future::{BoxFuture, FutureExt};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use std::path::Path;
use url::Url;

pub mod custom_scalars {
    pub type Semver = String;
}

pub use aqora_client::{CredentialsProvider, Error as GraphQLError};

pub type GraphQLClient = aqora_client::Client;

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

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

const AQORA_USER_AGENT: HeaderValue = HeaderValue::from_static(concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION")
));

pub async fn new(config_home: impl AsRef<Path>, url: Url) -> Result<GraphQLClient> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, AQORA_USER_AGENT);
    Ok(GraphQLClient::new(graphql_url(&url)?)
        .with_credentials(get_credentials(config_home, url.clone()).await?)
        .with_default_headers(headers))
}
