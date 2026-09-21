//! Credential caching and SigV4-signed object requests for the Python
//! `Store` and `KV`. Nothing in the CLI uses this yet.
#![cfg_attr(not(feature = "extension-module"), allow(dead_code))]

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use reqwest::{
    header::{
        HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE, ETAG, HOST, IF_MATCH, IF_NONE_MATCH,
    },
    Method, StatusCode,
};
use tokio::sync::Mutex;
use url::Url;

use super::{create_store_credentials, StoreCredentials};
use crate::{
    error::{self, Error, Result},
    graphql_client::{custom_scalars::DateTime, GraphQLClient},
    sigv4,
};

impl StoreCredentials {
    /// Seconds until the credentials expire; negative once they have.
    pub fn remaining_secs(&self, now: DateTime) -> f64 {
        (self.expires_at - now).num_milliseconds() as f64 / 1000.0
    }

    /// Path-style URL of `key` in the bucket, encoded the way SigV4 expects.
    pub fn object_url(&self, key: &str) -> Result<Url> {
        let path = sigv4::uri_encode(&format!("{}/{key}", self.bucket));
        Ok(Url::parse(&format!(
            "{}/{path}",
            self.endpoint.trim_end_matches('/')
        ))?)
    }
}

/// Where a [`Store`] gets fresh credentials from.
#[async_trait]
pub trait CredentialSource: Send + Sync {
    async fn mint(&self, duration_secs: Option<i64>) -> Result<StoreCredentials>;
}

#[async_trait]
impl CredentialSource for GraphQLClient {
    async fn mint(&self, duration_secs: Option<i64>) -> Result<StoreCredentials> {
        create_store_credentials(self, duration_secs).await
    }
}

/// The outcome of a conditional get.
#[derive(Debug)]
pub enum Fetched {
    Changed { body: Bytes, etag: String },
    NotModified,
    Missing,
}

#[derive(Debug, Clone)]
pub enum Precondition {
    /// Overwrite whatever is there.
    Any,
    IfMatch(String),
    IfNoneMatchAny,
}

#[derive(Debug)]
pub enum PutError {
    /// The precondition failed: someone else wrote the object first.
    Conflict,
    Other(Error),
}

impl From<Error> for PutError {
    fn from(error: Error) -> Self {
        Self::Other(error)
    }
}

/// The caller's bucket: credentials minted on demand and re-minted once fewer
/// than `refresh_margin` remain, plus SigV4-signed object requests on them.
pub struct Store {
    source: Arc<dyn CredentialSource>,
    duration: Option<i64>,
    refresh_margin: Duration,
    cache: Mutex<Option<StoreCredentials>>,
    http: reqwest::Client,
    allow_insecure_host: bool,
}

impl Store {
    pub fn new(
        source: Arc<dyn CredentialSource>,
        duration: Option<i64>,
        refresh_margin: Duration,
        http: reqwest::Client,
        allow_insecure_host: bool,
    ) -> Self {
        Self {
            source,
            duration,
            refresh_margin,
            cache: Mutex::new(None),
            http,
            allow_insecure_host,
        }
    }

    pub fn duration(&self) -> Option<i64> {
        self.duration
    }

    pub fn refresh_margin(&self) -> Duration {
        self.refresh_margin
    }

    /// Current credentials, minting new ones when needed. Callers that miss
    /// the cache together wait on the lock and mint once.
    pub async fn credentials(&self, force: bool) -> Result<StoreCredentials> {
        let mut cache = self.cache.lock().await;
        if !force {
            if let Some(creds) = cache.as_ref() {
                if creds.remaining_secs(Utc::now()) > self.refresh_margin.as_secs_f64() {
                    return Ok(creds.clone());
                }
            }
        }
        let creds = self.source.mint(self.duration).await?;
        *cache = Some(creds.clone());
        Ok(creds)
    }

    pub async fn invalidate(&self) {
        self.cache.lock().await.take();
    }

    pub async fn get_object(&self, key: &str, if_none_match: Option<&str>) -> Result<Fetched> {
        let mut headers = Vec::new();
        if let Some(etag) = if_none_match {
            headers.push((IF_NONE_MATCH, HeaderValue::from_str(etag)?));
        }
        let response = self
            .request(Method::GET, key, headers, Bytes::new())
            .await?;
        match response.status() {
            StatusCode::NOT_MODIFIED => Ok(Fetched::NotModified),
            StatusCode::NOT_FOUND => Ok(Fetched::Missing),
            StatusCode::OK => {
                let etag = etag_of(&response)?;
                let body = response.bytes().await?;
                Ok(Fetched::Changed { body, etag })
            }
            _ => Err(unexpected_status(response).await),
        }
    }

    /// Write `body` and return its new ETag.
    pub async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
        precondition: Precondition,
    ) -> Result<String, PutError> {
        let mut headers = vec![(
            CONTENT_TYPE,
            HeaderValue::from_str(content_type).map_err(Error::from)?,
        )];
        match precondition {
            Precondition::Any => {}
            Precondition::IfMatch(etag) => {
                headers.push((IF_MATCH, HeaderValue::from_str(&etag).map_err(Error::from)?));
            }
            Precondition::IfNoneMatchAny => {
                headers.push((IF_NONE_MATCH, HeaderValue::from_static("*")));
            }
        }
        let response = self.request(Method::PUT, key, headers, body).await?;
        match response.status() {
            StatusCode::PRECONDITION_FAILED => Err(PutError::Conflict),
            status if status.is_success() => Ok(etag_of(&response)?),
            _ => Err(unexpected_status(response).await.into()),
        }
    }

    /// Remove `key`; a missing object is not an error.
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        let response = self
            .request(Method::DELETE, key, Vec::new(), Bytes::new())
            .await?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(()),
            status if status.is_success() => Ok(()),
            _ => Err(unexpected_status(response).await),
        }
    }

    /// Send a signed request, re-minting once if the store rejects the
    /// credentials (they expired, or the login behind them changed).
    async fn request(
        &self,
        method: Method,
        key: &str,
        headers: Vec<(HeaderName, HeaderValue)>,
        body: Bytes,
    ) -> Result<reqwest::Response> {
        let payload_hash = sigv4::sha256_hex(&body);
        let mut force = false;
        loop {
            let creds = self.credentials(force).await?;
            let url = creds.object_url(key)?;
            if !(self.allow_insecure_host || aqora_client::utils::is_url_secure(&url)?) {
                return Err(error::user(
                    &format!("Store endpoint {} is insecure", creds.endpoint),
                    "Pass allow_insecure_host=True to the client to use it anyway",
                ));
            }
            let signature = sigv4::sign(sigv4::SigningInput {
                method: method.as_str(),
                url: &url,
                extra_headers: &[],
                payload_hash: &payload_hash,
                access_key_id: &creds.access_key_id,
                secret_access_key: &creds.secret_access_key,
                region: &creds.region,
                now: Utc::now(),
            });
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .header(HOST, sigv4::host_header(&url))
                .header("x-amz-date", signature.amz_date)
                .header("x-amz-content-sha256", &payload_hash)
                .header(AUTHORIZATION, signature.authorization)
                .body(body.clone());
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            let response = request.send().await?;
            let rejected = matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            );
            if rejected && !force {
                self.invalidate().await;
                force = true;
                continue;
            }
            return Ok(response);
        }
    }
}

fn etag_of(response: &reqwest::Response) -> Result<String> {
    response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| error::system("The store returned no ETag", ""))
}

async fn unexpected_status(response: reqwest::Response) -> Error {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    error::system(
        &format!("Store request failed with {status}: {}", body.trim()),
        "Check that you are logged in with storage access and try again",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn creds(endpoint: &str) -> StoreCredentials {
        StoreCredentials {
            access_key_id: "AQS1abc".into(),
            secret_access_key: "secret".into(),
            expires_at: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            endpoint: endpoint.into(),
            bucket: "alice".into(),
            region: "aqora".into(),
        }
    }

    #[test]
    fn object_urls_are_path_style_and_encoded() {
        assert_eq!(
            creds("http://100.123.32.73:3001")
                .object_url("path/to/item name.json")
                .unwrap()
                .as_str(),
            "http://100.123.32.73:3001/alice/path/to/item%20name.json"
        );
    }

    struct Mints {
        lifetime: i64,
        count: AtomicUsize,
    }

    #[async_trait]
    impl CredentialSource for Mints {
        async fn mint(&self, _duration: Option<i64>) -> Result<StoreCredentials> {
            let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(StoreCredentials {
                access_key_id: format!("AQS1key{n}"),
                expires_at: Utc::now() + chrono::Duration::seconds(self.lifetime),
                ..creds("https://s3.aqora.io")
            })
        }
    }

    fn store(lifetime: i64, margin: u64) -> (Arc<Mints>, Store) {
        let mints = Arc::new(Mints {
            lifetime,
            count: AtomicUsize::new(0),
        });
        let store = Store::new(
            mints.clone(),
            None,
            Duration::from_secs(margin),
            reqwest::Client::new(),
            false,
        );
        (mints, store)
    }

    #[tokio::test]
    async fn credentials_are_cached_inside_their_lifetime() {
        let (mints, store) = store(3600, 60);
        let first = store.credentials(false).await.unwrap();
        let second = store.credentials(false).await.unwrap();
        assert_eq!(first.access_key_id, second.access_key_id);
        assert_eq!(mints.count.load(Ordering::SeqCst), 1);
        assert_ne!(
            store.credentials(true).await.unwrap().access_key_id,
            first.access_key_id
        );
        assert_eq!(mints.count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn credentials_inside_the_refresh_margin_are_minted_again() {
        let (mints, store) = store(30, 60);
        store.credentials(false).await.unwrap();
        store.credentials(false).await.unwrap();
        assert_eq!(mints.count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_misses_mint_once() {
        let (mints, store) = store(3600, 60);
        let store = Arc::new(store);
        let tasks = (0..5)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.credentials(false).await.unwrap() })
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(mints.count.load(Ordering::SeqCst), 1);
    }
}
