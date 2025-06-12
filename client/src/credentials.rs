use async_trait::async_trait;
use reqwest::header::{HeaderName, AUTHORIZATION};
use reqwest::{Request, Response};

use crate::async_util::{MaybeSend, MaybeSync};
use crate::error::BoxError;
use crate::middleware::{Middleware, MiddlewareError, Next};

pub struct Tokens {
    access_token: Option<String>,
    refresh_token: Option<String>,
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
pub trait CredentialsProvider {
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
impl CredentialsProvider for tokio::sync::Mutex<Tokens> {
    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        let this = self.lock().await;
        Ok(this
            .access_token
            .as_ref()
            .or(this.refresh_token.as_ref())
            .map(|s| s.to_owned()))
    }
    async fn revoke_access_token(&self) -> Result<(), BoxError> {
        let _ = self.lock().await.access_token.take();
        Ok(())
    }
    async fn revoke_refresh_token(&self) -> Result<(), BoxError> {
        let _ = self.lock().await.refresh_token.take();
        Ok(())
    }
    async fn refresh(&self, tokens: Tokens) -> Result<(), BoxError> {
        let mut this = self.lock().await;
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

#[derive(Clone, Debug)]
pub struct CredentialsMiddleware<T>(T);

impl<T> CredentialsMiddleware<T> {
    pub fn new(credentials: T) -> Self {
        Self(credentials)
    }
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
impl<T> Middleware for CredentialsMiddleware<T>
where
    T: CredentialsProvider + MaybeSend + MaybeSync,
{
    async fn handle(&self, mut req: Request, next: Next<'_>) -> Result<Response, MiddlewareError> {
        let provider = &self.0;
        if let Some(token) = provider.bearer_token().await? {
            req.headers_mut()
                .insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
        }
        let res = next.handle(req).await?;
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
}
