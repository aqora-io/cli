use crate::error::Result;
use crate::login::get_access_token;
use graphql_client::{reqwest::post_graphql, GraphQLQuery, Response};
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use url::Url;

#[derive(Clone)]
pub struct GraphqlClient {
    client: reqwest::Client,
    url: Url,
}

impl GraphqlClient {
    pub async fn send<Q: GraphQLQuery>(
        &self,
        variables: Q::Variables,
    ) -> Result<Response<Q::ResponseData>, reqwest::Error> {
        post_graphql::<Q, _>(&self.client, self.url.clone(), variables).await
    }
}

pub async fn graphql_client(url: Url) -> Result<GraphqlClient> {
    let headers = if let Some(access_token) = get_access_token(&url).await? {
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
    Ok(GraphqlClient { client, url })
}
