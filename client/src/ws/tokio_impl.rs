use std::task::{Context, Poll};

use futures::{channel::mpsc, future::BoxFuture, FutureExt, SinkExt};
use tower::Service;

use crate::http::{Body, Request, Response};

use crate::error::MiddlewareError;

pub type Websocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub(crate) struct WsClient {
    sender: mpsc::Sender<Websocket>,
}

impl WsClient {
    pub fn new(sender: mpsc::Sender<Websocket>) -> Self {
        Self { sender }
    }
}

impl Service<Request> for WsClient {
    type Response = Response;
    type Error = MiddlewareError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Request) -> Self::Future {
        let mut sender = self.sender.clone();
        async move {
            let (parts, body) = req.into_parts();
            if body.as_bytes().is_none_or(|bytes| !bytes.is_empty()) {
                return Err(MiddlewareError::Middleware(
                    "Websocket request body must be empty".into(),
                ));
            }
            let req = http::Request::from_parts(parts, ());
            let (websocket, res) = tokio_tungstenite::connect_async(req)
                .await
                .map_err(|err| MiddlewareError::Ws(Box::new(err)))?;
            sender
                .send(websocket)
                .await
                .map_err(|err| MiddlewareError::Middleware(err.into()))?;
            let (parts, body) = res.into_parts();
            Ok(http::Response::from_parts(
                parts,
                body.map(Body::from).unwrap_or_default(),
            ))
        }
        .boxed()
    }
}
