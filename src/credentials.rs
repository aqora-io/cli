use crate::{
    error::Result,
    fs_lock::{ExclusiveLock, LockedFile, SharedLock},
};
use aqora_client::error::MiddlewareError;
use chrono::{DateTime, Duration, Utc};
use futures::{future::BoxFuture, prelude::*};
use graphql_client::GraphQLQuery;
use reqwest::header::{HeaderValue, AUTHORIZATION};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    task::{Context, Poll},
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use url::Url;

const EXPIRATION_PADDING_SEC: i64 = 60;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Credentials {
    pub client_id: String,
    pub client_secret: Option<String>,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

impl Credentials {
    fn is_expired(&self) -> bool {
        (self.expires_at - Duration::try_seconds(EXPIRATION_PADDING_SEC).unwrap()) <= Utc::now()
    }
}

#[inline]
fn base_url(url: &Url) -> Url {
    let mut url = url.clone();
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    url
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Default)]
pub struct CredentialsFile {
    pub credentials: HashMap<Url, Credentials>,
}

async fn read_file<T>(file: &mut LockedFile<T>) -> io::Result<CredentialsFile> {
    let mut buffer = String::new();
    file.rewind().await?;
    file.read_to_string(&mut buffer).await?;
    let trimmed = buffer.trim();
    if trimmed.is_empty() {
        Ok(CredentialsFile::default())
    } else {
        Ok(serde_json::from_str(trimmed)?)
    }
}

async fn write_file(
    file: &mut LockedFile<ExclusiveLock>,
    credentials_file: &CredentialsFile,
) -> std::io::Result<()> {
    let contents = serde_json::to_vec_pretty(credentials_file)?;
    file.rewind().await?;
    file.write_all(&contents).await?;
    file.set_len(contents.len() as u64).await?;
    file.sync_all().await?;
    Ok(())
}

pub type LoadCredentials = Option<(LockedFile<SharedLock>, Credentials)>;

pub async fn load_credentials(path: &Path, url: &Url) -> io::Result<LoadCredentials> {
    let mut file = match LockedFile::shared(path, OpenOptions::new().read(true)).await {
        Ok(file) => file,
        Err(error) => match error.kind() {
            io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(error),
        },
    };
    let credentials = read_file(&mut file)
        .await?
        .credentials
        .remove(&base_url(url));
    Ok(credentials.map(|credentials| (file, credentials)))
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_refresh.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2RefreshMutation;

async fn refresh_credentials(
    path: &Path,
    url: &Url,
    unauthenticated_client: &aqora_client::Client,
) -> io::Result<()> {
    let mut file = LockedFile::exclusive(
        path,
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false),
    )
    .await?;
    let mut credentials_file = read_file(&mut file).await?;
    let url = base_url(url);
    let Some(credentials) = credentials_file.credentials.remove(&url) else {
        return Ok(());
    };
    if !credentials.is_expired() {
        return Ok(());
    }
    let issued = unauthenticated_client
        .send::<Oauth2RefreshMutation>(oauth2_refresh_mutation::Variables {
            client_id: credentials.client_id.clone(),
            client_secret: credentials.client_secret.clone(),
            refresh_token: credentials.refresh_token,
        })
        .await?
        .oauth2_refresh
        .issued
        .ok_or_else(|| io::Error::other("GraphQL response missing issued"))?;
    credentials_file.credentials.insert(
        url,
        Credentials {
            client_id: credentials.client_id,
            client_secret: credentials.client_secret,
            access_token: issued.access_token,
            refresh_token: issued.refresh_token,
            expires_at: Utc::now() + Duration::try_seconds(issued.expires_in).unwrap(),
        },
    );
    write_file(&mut file, &credentials_file).await?;
    Ok(())
}

async fn load_refreshed_credentials(
    path: &Path,
    url: &Url,
    unauthenticated_client: &aqora_client::Client,
) -> io::Result<LoadCredentials> {
    if let Some((file, credentials)) = load_credentials(path, url).await? {
        if !credentials.is_expired() {
            return Ok(Some((file, credentials)));
        }
    } else {
        return Ok(None);
    }
    refresh_credentials(path, url, unauthenticated_client).await?;
    load_credentials(path, url).await
}

pub async fn insert_credentials(
    path: &Path,
    url: &Url,
    credentials: Credentials,
) -> io::Result<()> {
    let mut file = LockedFile::exclusive(
        path,
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false),
    )
    .await?;
    let mut credentials_file = read_file(&mut file).await?;
    credentials_file
        .credentials
        .insert(base_url(url), credentials);
    write_file(&mut file, &credentials_file).await?;
    Ok(())
}

#[derive(Clone)]
pub struct CredentialsFileService<S> {
    path: PathBuf,
    url: Url,
    unauthenticated_client: aqora_client::Client,
    inner: S,
}

impl<S> tower::Service<aqora_client::http::Request> for CredentialsFileService<S>
where
    S: tower::Service<aqora_client::http::Request, Error = MiddlewareError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = MiddlewareError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut req: aqora_client::http::Request) -> Self::Future {
        let mut this = self.clone();
        async move {
            if aqora_client::utils::host_matches(&this.url, &req.uri().to_string().parse::<Url>()?)?
            {
                if let Some((_guard, credentials)) =
                    load_refreshed_credentials(&this.path, &this.url, &this.unauthenticated_client)
                        .await?
                {
                    let token =
                        format!("Bearer {}", credentials.access_token).parse::<HeaderValue>()?;
                    req.headers_mut().insert(AUTHORIZATION, token);
                    return this.inner.call(req).await;
                }
            }
            this.inner.call(req).await
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct CredentialsFileLayer {
    path: PathBuf,
    url: Url,
    unauthenticated_client: aqora_client::Client,
}

impl CredentialsFileLayer {
    pub fn new(path: PathBuf, url: Url, unauthenticated_client: aqora_client::Client) -> Self {
        Self {
            path,
            url,
            unauthenticated_client,
        }
    }
}

impl<S> tower::Layer<S> for CredentialsFileLayer {
    type Service = CredentialsFileService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CredentialsFileService {
            path: self.path.clone(),
            url: self.url.clone(),
            unauthenticated_client: self.unauthenticated_client.clone(),
            inner,
        }
    }
}
