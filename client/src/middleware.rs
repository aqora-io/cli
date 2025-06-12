use std::sync::Arc;

use async_trait::async_trait;
use futures::prelude::*;

use crate::async_util::{MaybeLocalBoxFuture, MaybeSend, MaybeSync};

pub use reqwest::{Request, Response};

pub use crate::error::MiddlewareError;

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
pub trait Middleware: MaybeSend + MaybeSync {
    async fn handle(&self, req: Request, next: Next<'_>) -> Result<Response, MiddlewareError> {
        next.handle(req).await
    }
}

#[derive(Clone, Copy)]
pub struct Next<'a> {
    client: &'a reqwest::Client,
    middlewares: &'a [Arc<dyn Middleware>],
}

impl<'a> Next<'a> {
    pub fn new(client: &'a reqwest::Client, middlewares: &'a [Arc<dyn Middleware>]) -> Self {
        Self {
            client,
            middlewares,
        }
    }
}

impl<'a> Next<'a> {
    pub fn handle(
        mut self,
        req: Request,
    ) -> MaybeLocalBoxFuture<'a, Result<Response, MiddlewareError>> {
        if let Some((current, rest)) = self.middlewares.split_first() {
            self.middlewares = rest;
            current.handle(req, self)
        } else {
            Box::pin(self.client.execute(req).map_err(MiddlewareError::Request))
        }
    }
}

#[cfg(feature = "ws")]
mod ws {
    use super::*;
    pub use crate::error::WsMiddlewareError;
    use crate::ws::{connect, Websocket, WsRequest, WsResponse};

    #[derive(Clone, Copy)]
    pub struct WsNext<'a> {
        middlewares: &'a [Arc<dyn WsMiddleware>],
    }

    impl<'a> WsNext<'a> {
        pub fn new(middlewares: &'a [Arc<dyn WsMiddleware>]) -> Self {
            Self { middlewares }
        }
    }

    impl<'a> WsNext<'a> {
        pub fn handle(
            mut self,
            req: WsRequest,
        ) -> MaybeLocalBoxFuture<'a, Result<(Websocket, WsResponse), WsMiddlewareError>> {
            if let Some((current, rest)) = self.middlewares.split_first() {
                self.middlewares = rest;
                current.handle(req, self)
            } else {
                Box::pin(connect(req).map_err(WsMiddlewareError::Request))
            }
        }
    }

    #[cfg_attr(feature = "threaded", async_trait)]
    #[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
    pub trait WsMiddleware: MaybeSend + MaybeSync {
        async fn handle(
            &self,
            req: WsRequest,
            next: WsNext<'_>,
        ) -> Result<(Websocket, WsResponse), WsMiddlewareError>;
    }
}

#[cfg(feature = "ws")]
pub use ws::*;
