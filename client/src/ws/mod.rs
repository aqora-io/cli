mod tokio_impl;

use futures::{channel::mpsc, prelude::*};
use graphql_client::GraphQLQuery;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tower::{Layer, Service, ServiceExt};

use crate::async_util::{MaybeSend, MaybeSync};
use crate::client::{get_data, Client};
use crate::error::{Error, MiddlewareError, Result};
use crate::http::{Body, HttpBoxService, Request, Response};

pub(crate) use tokio_impl::{Websocket, WsClient};

impl Client {
    pub fn ws_layer<L, E>(&mut self, layer: L) -> &mut Self
    where
        L: Layer<HttpBoxService> + MaybeSend + MaybeSync + 'static,
        L::Service: Service<Request, Response = Response, Error = E> + Clone + MaybeSend + 'static,
        <L::Service as Service<Request>>::Future: MaybeSend + 'static,
        MiddlewareError: From<E>,
        E: 'static,
    {
        self.ws_layer.stack(layer);
        self
    }

    #[inline]
    fn ws_service(&self) -> (HttpBoxService, mpsc::Receiver<Websocket>) {
        let (sender, receiver) = futures::channel::mpsc::channel(1);
        (self.ws_layer.layer(WsClient::new(sender)), receiver)
    }

    pub async fn subscribe<Q>(
        &self,
        variables: Q::Variables,
    ) -> Result<impl Stream<Item = Result<Q::ResponseData>>>
    where
        Q: GraphQLQuery + Unpin + Send + 'static,
        Q::Variables: Unpin + Send,
    {
        let mut url = self.url().clone();
        if matches!(url.scheme(), "https") {
            url.set_scheme("wss").unwrap();
        } else {
            url.set_scheme("ws").unwrap();
        }
        let (parts, _) = tokio_tungstenite::tungstenite::client::ClientRequestBuilder::new(
            url.as_str().parse().unwrap(),
        )
        .with_sub_protocol("graphql-transport-ws")
        .into_client_request()?
        .into_parts();
        let request = http::Request::from_parts(parts, Body::default());
        let (service, mut receiver) = self.ws_service();
        let _ = service.oneshot(request).await?;
        let websocket = receiver.next().await.ok_or_else(|| Error::WsClosed)?;
        Ok(graphql_ws_client::Client::build(websocket)
            .subscribe(graphql_ws_client::graphql::StreamingOperation::<Q>::new(
                variables,
            ))
            .await?
            .map_err(|err| err.into())
            .and_then(|result| future::ready(get_data::<Q>(result))))
    }
}
