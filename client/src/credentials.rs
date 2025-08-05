use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use http::Uri;
use reqwest::header::{HeaderName, HeaderValue, AUTHORIZATION};
use tower::{Layer, Service};

use crate::async_util::{MaybeLocalBoxFuture, MaybeLocalFutureExt, MaybeSend, MaybeSync};
use crate::error::{BoxError, MiddlewareError};
// use crate::middleware::{Middleware, MiddlewareError, Next};

pub struct Tokens {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
pub trait CredentialsProvider {
    #[allow(unused_variables)]
    fn authenticates(&self, url: &Uri) -> bool {
        true
    }
    async fn bearer_token(&self) -> Result<Option<String>, BoxError>;
    async fn revoke_access_token(&self) -> Result<(), BoxError> {
        Ok(())
    }
    async fn revoke_refresh_token(&self) -> Result<(), BoxError> {
        Ok(())
    }
    #[allow(unused_variables)]
    async fn refresh(&self, tokens: Tokens) -> Result<(), BoxError> {
        Ok(())
    }
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
impl<T> CredentialsProvider for &T
where
    T: ?Sized + CredentialsProvider + MaybeSend + MaybeSync,
{
    fn authenticates(&self, url: &Uri) -> bool {
        T::authenticates(self, url)
    }
    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        T::bearer_token(self).await
    }
    async fn revoke_access_token(&self) -> Result<(), BoxError> {
        T::revoke_access_token(self).await
    }
    async fn revoke_refresh_token(&self) -> Result<(), BoxError> {
        T::revoke_refresh_token(self).await
    }
    async fn refresh(&self, tokens: Tokens) -> Result<(), BoxError> {
        T::refresh(self, tokens).await
    }
}

#[cfg(not(feature = "threaded"))]
#[async_trait(?Send)]
impl CredentialsProvider for std::cell::RefCell<Tokens> {
    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        let this = self.try_borrow_mut()?;
        Ok(this
            .access_token
            .as_ref()
            .or(this.refresh_token.as_ref())
            .map(|s| s.to_owned()))
    }
    async fn revoke_access_token(&self) -> Result<(), BoxError> {
        let _ = self.try_borrow_mut()?.access_token.take();
        Ok(())
    }
    async fn revoke_refresh_token(&self) -> Result<(), BoxError> {
        let _ = self.try_borrow_mut()?.refresh_token.take();
        Ok(())
    }
    async fn refresh(&self, tokens: Tokens) -> Result<(), BoxError> {
        let mut this = self.try_borrow_mut()?;
        if tokens.access_token.is_some() {
            this.access_token = tokens.access_token
        }
        if tokens.refresh_token.is_some() {
            this.refresh_token = tokens.refresh_token
        }
        Ok(())
    }
}

#[cfg(feature = "threaded")]
#[async_trait]
impl CredentialsProvider for tokio::sync::RwLock<Tokens> {
    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        let this = self.read().await;
        Ok(this
            .access_token
            .as_ref()
            .or(this.refresh_token.as_ref())
            .map(|s| s.to_owned()))
    }
    async fn revoke_access_token(&self) -> Result<(), BoxError> {
        let _ = self.write().await.access_token.take();
        Ok(())
    }
    async fn revoke_refresh_token(&self) -> Result<(), BoxError> {
        let _ = self.write().await.refresh_token.take();
        Ok(())
    }
    async fn refresh(&self, tokens: Tokens) -> Result<(), BoxError> {
        let mut this = self.write().await;
        if tokens.access_token.is_some() {
            this.access_token = tokens.access_token
        }
        if tokens.refresh_token.is_some() {
            this.refresh_token = tokens.refresh_token
        }
        Ok(())
    }
}

const X_REVOKE_TOKENS: HeaderName = HeaderName::from_static("x-revoke-tokens");
const X_REFRESH_TOKENS: HeaderName = HeaderName::from_static("x-refresh-tokens");
const X_ACCESS_TOKEN: HeaderName = HeaderName::from_static("x-access-token");
const X_REFRESH_TOKEN: HeaderName = HeaderName::from_static("x-refresh-token");

pub struct CredentialsService<T, S> {
    credentials: Arc<T>,
    ready: CredentialsServiceReady,
    inner: S,
}

impl<T, S> Clone for CredentialsService<T, S>
where
    T: CredentialsProvider + MaybeSend + MaybeSync + 'static,
    S: Clone,
{
    fn clone(&self) -> Self {
        Self::new_arc(self.credentials.clone(), self.inner.clone())
    }
}

struct CredentialsServiceReady {
    inner_ready: bool,
    bearer_fut: MaybeLocalBoxFuture<'static, Result<Option<String>, BoxError>>,
    bearer: Poll<Option<HeaderValue>>,
}

impl<T, S> CredentialsService<T, S>
where
    T: CredentialsProvider + MaybeSend + MaybeSync + 'static,
{
    pub fn new(credentials: T, service: S) -> Self {
        Self::new_arc(Arc::new(credentials), service)
    }
    fn new_arc(credentials: Arc<T>, service: S) -> Self {
        Self {
            credentials: credentials.clone(),
            ready: CredentialsServiceReady {
                inner_ready: false,
                bearer_fut: async move { credentials.bearer_token().await }.boxed_maybe_local(),
                bearer: Poll::Pending,
            },
            inner: service,
        }
    }
}

impl<T, S, F, E> Service<crate::http::Request> for CredentialsService<T, S>
where
    T: CredentialsProvider + MaybeSend + MaybeSync + 'static,
    S: Service<crate::http::Request, Response = crate::http::Response, Error = E, Future = F>,
    MiddlewareError: From<E>,
    F: Future<Output = Result<crate::http::Response, E>> + MaybeSend + 'static,
{
    type Response = S::Response;
    type Error = MiddlewareError;
    type Future = MaybeLocalBoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), MiddlewareError>> {
        let inner_ready = if self.ready.inner_ready {
            true
        } else {
            match self.inner.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    self.ready.inner_ready = true;
                    true
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(MiddlewareError::from(err)));
                }
                Poll::Pending => false,
            }
        };
        let bearer_ready = if self.ready.bearer.is_ready() {
            true
        } else {
            match self.ready.bearer_fut.as_mut().poll(cx) {
                Poll::Ready(Ok(token)) => {
                    let token = match token
                        .map(|token| format!("Bearer {token}").parse::<HeaderValue>())
                        .transpose()
                    {
                        Ok(token) => token,
                        Err(err) => {
                            return Poll::Ready(Err(MiddlewareError::Middleware(err.into())));
                        }
                    };
                    self.ready.bearer = Poll::Ready(token);
                    true
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(MiddlewareError::Middleware(err)));
                }
                Poll::Pending => false,
            }
        };
        if inner_ready && bearer_ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn call(&mut self, mut req: crate::http::Request) -> Self::Future {
        if !self.credentials.authenticates(req.uri()) {
            let fut = self.inner.call(req);
            return async move { Ok(fut.await?) }.boxed_maybe_local();
        }

        if let Poll::Ready(Some(token)) = &self.ready.bearer {
            req.headers_mut().insert(AUTHORIZATION, token.clone());
        }
        let provider = self.credentials.clone();
        let fut = self.inner.call(req);
        async move {
            let res = fut.await?;
            if res.headers().contains_key(X_REVOKE_TOKENS) {
                provider.revoke_access_token().await?;
                provider.revoke_refresh_token().await?;
            }
            if res.headers().contains_key(X_REFRESH_TOKENS) {
                provider.revoke_access_token().await?;
            }
            let access_token = res.headers().get(X_ACCESS_TOKEN);
            let refresh_token = res.headers().get(X_REFRESH_TOKEN);
            if access_token.is_some() || refresh_token.is_some() {
                let tokens = Tokens {
                    access_token: access_token
                        .map(|token| token.to_str().map(|s| s.to_string()))
                        .transpose()?,
                    refresh_token: refresh_token
                        .map(|token| token.to_str().map(|s| s.to_string()))
                        .transpose()?,
                };
                provider.refresh(tokens).await?;
            }
            Ok(res)
        }
        .boxed_maybe_local()
    }
}

pub struct CredentialsLayer<T> {
    credentials: Arc<T>,
}

impl<T> Clone for CredentialsLayer<T> {
    fn clone(&self) -> Self {
        Self {
            credentials: self.credentials.clone(),
        }
    }
}

impl<T> CredentialsLayer<T> {
    pub fn new(credentials: T) -> Self {
        Self {
            credentials: Arc::new(credentials),
        }
    }
}

impl<T, S> Layer<S> for CredentialsLayer<T>
where
    T: CredentialsProvider + MaybeSend + MaybeSync + 'static,
{
    type Service = CredentialsService<T, S>;
    fn layer(&self, inner: S) -> Self::Service {
        CredentialsService::new_arc(self.credentials.clone(), inner)
    }
}
