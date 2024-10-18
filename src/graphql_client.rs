use crate::{
    credentials::{get_credentials, Credentials},
    error::{self, Error, Result},
};
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

impl GraphQLClient {
    pub async fn new(url: Url) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, "aqora".parse()?);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(Self {
            client,
            url: graphql_url(&url)?,
            credentials: get_credentials(url.clone()).await?,
        })
    }

    pub fn inner(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn send<Q: GraphQLQuery>(
        &self,
        variables: Q::Variables,
    ) -> Result<Q::ResponseData, GraphQLError> {
        let response = self.post_graphql::<Q>(variables).await?;
        if let Some(data) = response.data {
            Ok(data)
        } else if let Some(errors) = response.errors {
            Err(GraphQLError::Response(errors))
        } else {
            Err(GraphQLError::NoData)
        }
    }

    async fn post_graphql<Q: GraphQLQuery>(
        &self,
        variables: Q::Variables,
    ) -> Result<graphql_client::Response<Q::ResponseData>, GraphQLError> {
        let mut headers = HeaderMap::new();

        if let Some(credentials) = &self.credentials {
            if let Some(credentials) = credentials.clone().refresh(&self.url).await? {
                headers.insert(
                    AUTHORIZATION,
                    format!("Bearer {}", credentials.access_token).parse()?,
                );
            }
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
}
