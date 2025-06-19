use std::sync::Arc;

use futures::prelude::*;
use graphql_client::GraphQLQuery;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::client::{get_data, Client};
use crate::error::Result;
use crate::middleware::{WsMiddleware, WsNext};

pub use tokio_tungstenite::tungstenite::handshake::client::{
    Request as WsRequest, Response as WsResponse,
};

pub type Websocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub(crate) async fn connect(
    req: WsRequest,
) -> Result<(Websocket, WsResponse), tokio_tungstenite::tungstenite::error::Error> {
    tokio_tungstenite::connect_async(req).await
}

impl Client {
    #[inline]
    pub fn ws_with(&mut self, middleware: impl WsMiddleware + 'static) -> &mut Self {
        self.ws_middleware.push(Arc::new(middleware));
        self
    }

    #[inline]
    pub fn ws_with_arc(&mut self, middleware: Arc<dyn WsMiddleware>) -> &mut Self {
        self.ws_middleware.push(middleware);
        self
    }

    #[inline]
    pub fn ws_middleware(&self) -> &[Arc<dyn WsMiddleware>] {
        &self.ws_middleware
    }

    #[inline]
    pub fn ws_middleware_mut(&mut self) -> &mut [Arc<dyn WsMiddleware>] {
        &mut self.ws_middleware
    }

    #[inline]
    fn ws_next(&self) -> WsNext {
        WsNext::new(&self.ws_middleware)
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
        let request = tokio_tungstenite::tungstenite::client::ClientRequestBuilder::new(
            url.as_str().parse().unwrap(),
        )
        .with_sub_protocol("graphql-transport-ws")
        .into_client_request()?;
        let (websocket, _) = self.ws_next().handle(request).await?;
        Ok(graphql_ws_client::Client::build(websocket)
            .subscribe(graphql_ws_client::graphql::StreamingOperation::<Q>::new(
                variables,
            ))
            .await?
            .map_err(|err| err.into())
            .and_then(|result| future::ready(get_data::<Q>(result))))
    }
}
