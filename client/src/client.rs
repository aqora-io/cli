use std::fmt;
use std::sync::Arc;

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
    headers: HeaderMap,
) -> Result<graphql_client::Response<Q::ResponseData>> {
    let body = Q::build_query(variables);
    tracing::debug!("sending request: {}", serde_json::to_string(&body)?);
    let reqwest_response = client.post(url).headers(headers).json(&body).send().await?;

    let json: serde_json::Value = reqwest_response.json().await?;
    tracing::debug!("received response: {}", serde_json::to_string(&json)?);
    Ok(serde_json::from_value(json)?)
}

async fn send<Q: GraphQLQuery>(
    client: &reqwest::Client,
    url: Url,
    variables: Q::Variables,
    headers: HeaderMap,
) -> Result<Q::ResponseData> {
    get_data::<Q>(post_graphql::<Q>(client, url, variables, headers).await?)
}

#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    url: Url,
    default_headers: HeaderMap,
    credentials: Arc<dyn CredentialsProvider>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("client", &self.client)
            .field("url", &self.url)
            .finish()
    }
}

impl Client {
    pub fn new(url: Url) -> Self {
        Client {
            client: reqwest::Client::new(),
            url,
            default_headers: HeaderMap::new(),
            credentials: Arc::new(Option::<String>::None),
        }
    }

    pub fn with_credentials(mut self, credentials: impl CredentialsProvider + 'static) -> Self {
        self.credentials = Arc::new(credentials);
        self
    }

    pub fn with_default_headers(mut self, headers: HeaderMap) -> Self {
        self.default_headers = headers;
        self
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
    pub fn default_headers(&self) -> &HeaderMap {
        &self.default_headers
    }

    #[inline]
    pub fn credentials(&self) -> impl CredentialsProvider + '_ {
        self.credentials.as_ref()
    }
}

impl Client {
    pub(crate) async fn bearer_token(&self) -> Result<Option<String>> {
        Ok(self
            .credentials
            .access_token(&self.url)
            .await
            .map_err(Error::Credentials)?
            .map(|access_token| format!("Bearer {access_token}")))
    }

    #[inline]
    pub async fn send<Q: GraphQLQuery>(&self, variables: Q::Variables) -> Result<Q::ResponseData> {
        let mut headers = self.default_headers.clone();
        if let Some(token) = self.bearer_token().await? {
            headers.insert(AUTHORIZATION, token.parse()?);
        }
        send::<Q>(&self.client, self.url.clone(), variables, headers).await
    }
}
