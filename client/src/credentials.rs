use futures::prelude::*;
use reqwest::header::AUTHORIZATION;
use reqwest::{Request, Response};
use url::Url;

use crate::async_util::{MaybeLocalBoxFuture, MaybeLocalFutureExt, MaybeSend, MaybeSync};
use crate::error::BoxError;
use crate::middleware::{Middleware, MiddlewareError, Next};

pub trait CredentialsProvider: MaybeSend + MaybeSync {
    fn access_token<'a>(
        &'a self,
        url: &Url,
    ) -> MaybeLocalBoxFuture<'a, Result<Option<String>, BoxError>>;
}

impl<T> CredentialsProvider for &T
where
    T: ?Sized + CredentialsProvider,
{
    fn access_token<'a>(
        &'a self,
        url: &Url,
    ) -> MaybeLocalBoxFuture<'a, Result<Option<String>, BoxError>> {
        T::access_token(self, url)
    }
}

impl CredentialsProvider for String {
    fn access_token<'a>(
        &'a self,
        _: &Url,
    ) -> MaybeLocalBoxFuture<'a, Result<Option<String>, BoxError>> {
        futures::future::ok(Some(self.clone())).boxed_maybe_local()
    }
}

impl<T> CredentialsProvider for Option<T>
where
    T: CredentialsProvider + Send + Sync,
{
    fn access_token<'a>(
        &'a self,
        url: &Url,
    ) -> MaybeLocalBoxFuture<'a, Result<Option<String>, BoxError>> {
        if let Some(creds) = self.as_ref() {
            T::access_token(creds, url)
        } else {
            futures::future::ok(None).boxed_maybe_local()
        }
    }
}

fn apply_access_token(
    req: &mut Request,
    access_token: Option<String>,
) -> Result<(), MiddlewareError> {
    if let Some(token) = access_token {
        req.headers_mut()
            .insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct CredentialsMiddleware<T>(T);

impl<T> CredentialsMiddleware<T> {
    pub fn new(credentials: T) -> Self {
        Self(credentials)
    }
}

impl<T> Middleware for CredentialsMiddleware<T>
where
    T: CredentialsProvider,
{
    fn handle<'a>(
        &'a self,
        mut req: Request,
        next: Next<'a>,
    ) -> MaybeLocalBoxFuture<'a, Result<Response, MiddlewareError>> {
        self.0
            .access_token(req.url())
            .map_err(MiddlewareError::from)
            .and_then(|access_token| {
                futures::future::ready(apply_access_token(&mut req, access_token).map(|_| req))
            })
            .and_then(move |req| next.handle(req))
            .boxed_maybe_local()
    }
}
