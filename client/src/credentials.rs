use url::Url;

use crate::async_util::{MaybeLocalBoxFuture, MaybeLocalFutureExt, MaybeSend, MaybeSync};
use crate::error::BoxError;

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
