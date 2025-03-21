use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use reqwest::header::{HeaderMap, AUTHORIZATION, USER_AGENT};
use thiserror::Error;
use url::Url;

pub mod custom_scalars {
    pub type Semver = String;
}

#[derive(Error, Debug)]
pub enum GraphQLError {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("GraphQL response contained errors: {0:?}")]
    Response(Vec<graphql_client::Error>),
    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
    #[error(transparent)]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::error::Error),
    #[error(transparent)]
    GraphQLWs(#[from] graphql_ws_client::Error),
    #[error("GraphQL response contained no data")]
    NoData,
    #[error(transparent)]
    Other(#[from] Error),
}

impl From<GraphQLError> for Error {
    fn from(error: GraphQLError) -> Self {
        match error {
            GraphQLError::Request(error) => {
                error::system(&format!("Request failed: {error:?}"), "")
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
            GraphQLError::Other(other) => other,
            GraphQLError::InvalidHeaderValue(_) => {
                error::system("Invalid header value from client", "")
            }
        }
    }
}

#[derive(Clone)]
pub struct GraphQLClient {
    client: reqwest::Client,
    url: Url,
    credentials: Option<Credentials>,
}

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

fn get_data<Q: GraphQLQuery>(
    response: graphql_client::Response<Q::ResponseData>,
) -> Result<Q::ResponseData, GraphQLError> {
    if let Some(data) = response.data {
        Ok(data)
    } else if let Some(errors) = response.errors {
        Err(GraphQLError::Response(errors))
    } else {
        Err(GraphQLError::NoData)
    }
}

impl GraphQLClient {
    fn with_creds(url: Url, credentials: Option<Credentials>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, "aqora".parse()?);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(Self {
            client,
            url: graphql_url(&url)?,
            credentials,
        })
    }
    pub async fn new(url: Url) -> Result<Self> {
        Self::with_creds(url.clone(), get_credentials(url).await?)
    }

    pub fn no_creds(url: Url) -> Result<Self> {
        Self::with_creds(url, None)
    }

    async fn bearer_token(&self) -> Result<Option<String>, GraphQLError> {
        if let Some(credentials) = &self.credentials {
            if let Some(credentials) = credentials.clone().refresh(&self.url).await? {
                return Ok(Some(format!("Bearer {}", credentials.access_token)));
            }
        }
        Ok(None)
    }

    pub fn inner(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn send<Q: GraphQLQuery>(
        &self,
        variables: Q::Variables,
    ) -> Result<Q::ResponseData, GraphQLError> {
        get_data::<Q>(self.post_graphql::<Q>(variables).await?)
    }

    async fn post_graphql<Q: GraphQLQuery>(
        &self,
        variables: Q::Variables,
    ) -> Result<graphql_client::Response<Q::ResponseData>, GraphQLError> {
        let mut headers = HeaderMap::new();

        if let Some(token) = &self.bearer_token().await? {
            headers.insert(AUTHORIZATION, token.parse()?);
        }

        let body = Q::build_query(variables);
        let reqwest_response = self
            .client
            .post(self.url.clone())
            .headers(headers)
            .json(&body)
            .send()
            .await?;

        Ok(reqwest_response.json().await?)
    }

    pub async fn subscribe<Q>(
        &self,
        variables: Q::Variables,
    ) -> Result<impl Stream<Item = Result<Q::ResponseData, GraphQLError>>, GraphQLError>
    where
        Q: GraphQLQuery + Unpin + Send + Sync + 'static,
        Q::Variables: Unpin + Send + Sync,
    {
        let mut url = self.url.clone();
        if matches!(url.scheme(), "https") {
            url.set_scheme("wss").unwrap();
        } else {
            url.set_scheme("ws").unwrap();
        }
        // @NOTE: At the moment we don't support authorization on subscriptions
        let mut request = tokio_tungstenite::tungstenite::client::ClientRequestBuilder::new(
            url.as_str().parse().unwrap(),
        )
        .with_sub_protocol("graphql-transport-ws");
        if let Some(token) = &self.bearer_token().await? {
            request = request.with_header("Authorization", token);
        }
        let (websocket, _) = tokio_tungstenite::connect_async(request).await?;
        Ok(graphql_ws_client::Client::build(websocket)
            .subscribe(graphql_ws_client::graphql::StreamingOperation::<Q>::new(
                variables,
            ))
            .await?
            .map_err(|err| err.into())
            .and_then(|result| future::ready(get_data::<Q>(result))))
    }
}
