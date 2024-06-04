use crate::credentials::get_access_token;
use crate::error::{self, Error, Result};
use graphql_client::{reqwest::post_graphql, GraphQLQuery};
use reqwest::header::{AUTHORIZATION, USER_AGENT};
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
    #[error("GraphQL response contained no data")]
    NoData,
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
        }
    }
}

#[derive(Clone)]
pub struct GraphQLClient {
    client: reqwest::Client,
    url: Url,
}

pub fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

impl GraphQLClient {
    pub async fn new(url: Url) -> Result<Self> {
        let headers = if let Some(access_token) = get_access_token(url.clone()).await? {
            [
                (AUTHORIZATION, format!("Bearer {}", access_token).parse()?),
                (USER_AGENT, "aqora".parse()?),
            ]
            .into_iter()
            .collect()
        } else {
            Default::default()
        };
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(Self {
            client,
            url: graphql_url(&url)?,
        })
    }

    pub fn inner(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn send<Q: GraphQLQuery>(
        &self,
        variables: Q::Variables,
    ) -> Result<Q::ResponseData, GraphQLError> {
        let response = post_graphql::<Q, _>(&self.client, self.url.clone(), variables).await?;
        if let Some(data) = response.data {
            Ok(data)
        } else if let Some(errors) = response.errors {
            Err(GraphQLError::Response(errors))
        } else {
            Err(GraphQLError::NoData)
        }
    }
}
