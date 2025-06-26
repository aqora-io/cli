use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::TryFutureExt;
use tower::{Layer, Service};

use crate::async_util::{MaybeLocalBoxFuture, MaybeLocalFutureExt};
use crate::error::MiddlewareError;
use crate::tower_util::{ArcLayer, BoxService};

#[derive(Clone)]
pub(crate) struct HttpClient {
    client: reqwest::Client,
}

impl HttpClient {
    pub(crate) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

pub type Request = http::Request<reqwest::Body>;
pub type Response = http::Response<reqwest::Body>;

impl Service<Request> for HttpClient {
    type Response = Response;
    type Error = MiddlewareError;
    type Future = MaybeLocalBoxFuture<'static, Result<Response, MiddlewareError>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), MiddlewareError>> {
        self.client.poll_ready(cx).map_err(MiddlewareError::Request)
    }
    fn call(&mut self, req: Request) -> Self::Future {
        let client = self.client.clone();
        async move { Ok(client.execute(req.try_into()?).await?.into()) }
            .map_err(MiddlewareError::Request)
            .boxed_maybe_local()
    }
}

pub(crate) type HttpBoxService = BoxService<Request, Response, MiddlewareError>;
pub(crate) type HttpArcLayer<Client> = ArcLayer<Client, Request, Response, MiddlewareError>;

#[derive(Clone, Debug)]
pub struct NormalizeHttpService<S> {
    inner: S,
}

impl<S> NormalizeHttpService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ResBody> Service<Request> for NormalizeHttpService<S>
where
    S: Service<Request, Response = http::Response<ResBody>>,
    ResBody: http_body::Body + Send + Sync + 'static,
    ResBody::Data: Into<bytes::Bytes>,
    ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response;
    type Error = S::Error;
    type Future = futures::future::MapOk<S::Future, fn(http::Response<ResBody>) -> Response>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req).map_ok(|res| {
            let (parts, body) = res.into_parts();
            http::Response::from_parts(parts, reqwest::Body::wrap(body))
        })
    }
}

pub struct NormalizeHttpLayer<L> {
    layer: Arc<L>,
}

impl<L> NormalizeHttpLayer<L> {
    pub fn new(layer: L) -> Self {
        Self {
            layer: Arc::new(layer),
        }
    }
}

impl<L> Clone for NormalizeHttpLayer<L> {
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
        }
    }
}

impl<L, S> Layer<S> for NormalizeHttpLayer<L>
where
    L: Layer<S>,
{
    type Service = NormalizeHttpService<L::Service>;
    fn layer(&self, inner: S) -> Self::Service {
        NormalizeHttpService::new(self.layer.layer(inner))
    }
}
