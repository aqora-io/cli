use std::convert::Infallible;
use url::Url;

#[async_trait::async_trait]
pub trait CredentialsProvider {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn access_token(&self, url: &Url) -> Result<Option<String>, Self::Error>;
}

#[async_trait::async_trait]
impl CredentialsProvider for String {
    type Error = Infallible;
    async fn access_token(&self, _url: &Url) -> Result<Option<String>, Self::Error> {
        Ok(Some(self.clone()))
    }
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for Option<T>
where
    T: CredentialsProvider + Send + Sync,
{
    type Error = T::Error;
    async fn access_token(&self, url: &Url) -> Result<Option<String>, Self::Error> {
        if let Some(creds) = self.as_ref() {
            creds.access_token(url).await
        } else {
            Ok(None)
        }
    }
}
