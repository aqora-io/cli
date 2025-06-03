use futures::prelude::*;
use graphql_client::GraphQLQuery;

use crate::client::{get_data, Client};
use crate::credentials::CredentialsProvider;
use crate::error::Result;

impl<C> Client<C>
where
    C: CredentialsProvider,
{
    pub async fn subscribe<Q>(
        &self,
        variables: Q::Variables,
    ) -> Result<impl Stream<Item = Result<Q::ResponseData>>>
    where
        Q: GraphQLQuery + Unpin + Send + Sync + 'static,
        Q::Variables: Unpin + Send + Sync,
    {
        let mut url = self.url().clone();
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
