use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::TryFutureExt;
use tower::{Layer, Service, ServiceExt};

use crate::async_util::{MaybeLocalBoxFuture, MaybeSend, MaybeSync};

pub trait MaybeLocalService<T, U, E>:
    Service<T, Response = U, Error = E, Future = MaybeLocalBoxFuture<'static, Result<U, E>>> + MaybeSend
{
    fn clone_box(&self) -> Box<dyn MaybeLocalService<T, U, E>>;
}

impl<T, U, E, S> MaybeLocalService<T, U, E> for S
where
    S: Service<T, Response = U, Error = E, Future = MaybeLocalBoxFuture<'static, Result<U, E>>>
        + Clone
        + MaybeSend
        + 'static,
{
    fn clone_box(&self) -> Box<dyn MaybeLocalService<T, U, E>> {
        Box::new(self.clone())
    }
}

pub struct BoxService<T, U, E> {
    inner: Box<dyn MaybeLocalService<T, U, E>>,
}

impl<T, U, E> BoxService<T, U, E> {
    pub fn new<S, SE>(inner: S) -> Self
    where
        S: Service<T, Response = U, Error = SE> + Clone + MaybeSend + 'static,
        E: From<SE> + 'static,
        SE: 'static,
        S::Future: MaybeSend + 'static,
    {
        let service = inner.map_future(|f: S::Future| Box::pin(f.map_err(From::from)) as _);
        BoxService {
            inner: Box::new(service),
        }
    }
}

impl<T, U, E> Clone for BoxService<T, U, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

impl<T, U, E> Service<T> for BoxService<T, U, E> {
    type Response = U;
    type Error = E;
    type Future = MaybeLocalBoxFuture<'static, Result<U, E>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: T) -> MaybeLocalBoxFuture<'static, Result<U, E>> {
        self.inner.call(request)
    }
}

pub trait MaybeLocalLayer<In, T, U, E>:
    Layer<In, Service = BoxService<T, U, E>> + MaybeSend + MaybeSync
{
}

impl<In, T, U, E, L> MaybeLocalLayer<In, T, U, E> for L where
    L: Layer<In, Service = BoxService<T, U, E>> + MaybeSend + MaybeSync
{
}

pub struct ArcLayer<In, T, U, E> {
    inner: Arc<dyn MaybeLocalLayer<In, T, U, E> + 'static>,
}

impl<In, T, U, E> Clone for ArcLayer<In, T, U, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<In, T, U, E> ArcLayer<In, T, U, E> {
    pub fn new<L, SE>(inner_layer: L) -> Self
    where
        SE: 'static,
        E: From<SE> + 'static,
        L: Layer<In> + MaybeSend + MaybeSync + 'static,
        L::Service: Service<T, Response = U, Error = SE> + Clone + MaybeSend + 'static,
        <L::Service as Service<T>>::Future: MaybeSend + 'static,
    {
        let layer = tower::layer::layer_fn(move |inner: In| {
            let out = inner_layer.layer(inner);
            BoxService::new(out)
        });
        Self {
            inner: Arc::new(layer),
        }
    }

    pub fn stack<L, SE>(&mut self, outer_layer: L) -> &mut Self
    where
        In: 'static,
        T: 'static,
        U: 'static,
        SE: 'static,
        E: From<SE> + 'static,
        L: Layer<BoxService<T, U, E>> + MaybeSend + MaybeSync + 'static,
        L::Service: Service<T, Response = U, Error = SE> + Clone + MaybeSend + 'static,
        <L::Service as Service<T>>::Future: MaybeSend + 'static,
    {
        let inner_layer = self.clone();
        let layer = tower::layer::layer_fn(move |inner: In| {
            let out = outer_layer.layer(inner_layer.layer(inner));
            BoxService::new(out)
        });
        *self = Self::new(layer);
        self
    }
}

impl<In, T, U, E> Default for ArcLayer<In, T, U, E>
where
    In: Service<T, Response = U, Error = E> + Clone + MaybeSend + 'static,
    E: 'static,
    In::Future: MaybeSend + 'static,
{
    fn default() -> Self {
        let layer = tower::layer::layer_fn(move |inner: In| BoxService::new(inner));
        Self {
            inner: Arc::new(layer),
        }
    }
}

impl<In, T, U, E> Layer<In> for ArcLayer<In, T, U, E> {
    type Service = BoxService<T, U, E>;

    fn layer(&self, inner: In) -> Self::Service {
        self.inner.layer(inner)
    }
}
