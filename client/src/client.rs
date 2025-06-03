use graphql_client::GraphQLQuery;
use reqwest::header::{HeaderMap, AUTHORIZATION};
use url::Url;

use crate::credentials::CredentialsProvider;
use crate::error::{Error, Result};

pub(crate) fn get_data<Q: GraphQLQuery>(
    response: graphql_client::Response<Q::ResponseData>,
) -> Result<Q::ResponseData> {
    if let Some(data) = response.data {
        Ok(data)
    } else if let Some(errors) = response.errors {
        Err(Error::Response(errors))
    } else {
        Err(Error::NoData)
    }
}

async fn post_graphql<Q: GraphQLQuery>(
    client: &reqwest::Client,
    url: Url,
    variables: Q::Variables,
    bearer_token: Option<String>,
) -> Result<graphql_client::Response<Q::ResponseData>> {
    let mut headers = HeaderMap::new();

    if let Some(token) = bearer_token {
        headers.insert(AUTHORIZATION, token.parse()?);
    }

    let body = Q::build_query(variables);
    tracing::debug!("sending request: {}", serde_json::to_string(&body)?);
    let reqwest_response = client.post(url).headers(headers).json(&body).send().await?;

    let json: serde_json::Value = reqwest_response.json().await?;
    tracing::debug!("received response: {}", serde_json::to_string(&json)?);
    Ok(serde_json::from_value(json)?)
}

pub async fn send<Q: GraphQLQuery>(
    client: &reqwest::Client,
    url: Url,
    variables: Q::Variables,
    bearer_token: Option<String>,
) -> Result<Q::ResponseData> {
    get_data::<Q>(post_graphql::<Q>(client, url, variables, bearer_token).await?)
}

#[derive(Clone, Debug)]
pub struct Client<C> {
    client: reqwest::Client,
    url: Url,
    credentials: C,
}

impl<C> Client<C> {
    pub fn new_with_client(client: reqwest::Client, url: Url, credentials: C) -> Self {
        Client {
            client,
            url,
            credentials,
        }
    }

    pub fn new(url: Url, credentials: C) -> Self {
        Client::new_with_client(reqwest::Client::new(), url, credentials)
    }

    #[inline]
    pub fn inner(&self) -> &reqwest::Client {
        &self.client
    }

    #[inline]
    pub fn url(&self) -> &Url {
        &self.url
    }

    #[inline]
    pub fn credentials(&self) -> &C {
        &self.credentials
    }
}

impl Client<Option<String>> {
    #[inline]
    pub fn unauthenticated(url: Url) -> Self {
        Self::new(url, None)
    }
}

impl<C> Client<C>
where
    C: CredentialsProvider,
{
    pub(crate) async fn bearer_token(&self) -> Result<Option<String>> {
        Ok(self
            .credentials
            .access_token(&self.url)
            .await
            .map_err(|err| Error::Credentials(Box::new(err)))?
            .map(|access_token| format!("Bearer {access_token}")))
    }

    #[inline]
    pub async fn send<Q: GraphQLQuery>(&self, variables: Q::Variables) -> Result<Q::ResponseData> {
        send::<Q>(
            &self.client,
            self.url.clone(),
            variables,
            self.bearer_token().await?,
        )
        .await
    }
}
