use crate::{
    dirs::credentials_path,
    error::{self, Result},
    graphql_client::unauthenticated_client,
};
use chrono::{DateTime, Duration, Utc};
use fs4::tokio::AsyncFileExt;
use futures::{future::BoxFuture, prelude::*};
use graphql_client::GraphQLQuery;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path};
use tokio::{
    fs::{File, OpenOptions},
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
    pub async fn refresh(self, url: &Url) -> error::Result<Option<Self>> {
        if (self.expires_at - Duration::try_seconds(EXPIRATION_PADDING_SEC).unwrap()) > Utc::now() {
            return Ok(Some(self));
        }

        let issued = unauthenticated_client(url.clone())?
            .send::<Oauth2RefreshMutation>(oauth2_refresh_mutation::Variables {
                client_id: self.client_id.clone(),
                client_secret: self.client_secret.clone(),
                refresh_token: self.refresh_token,
            })
            .await?
            .oauth2_refresh
            .issued
            .ok_or_else(|| {
                error::system(
                    "GraphQL response missing issued",
                    "This is a bug, please report it",
                )
            })?;

        Ok(Some(Credentials {
            client_id: self.client_id,
            client_secret: self.client_secret,
            access_token: issued.access_token,
            refresh_token: issued.refresh_token,
            expires_at: Utc::now() + Duration::try_seconds(issued.expires_in).unwrap(),
        }))
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_token.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2TokenMutation;

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub struct CredentialsFile {
    pub credentials: HashMap<Url, Credentials>,
}

async fn replace_file(file: &mut File, contents: impl AsRef<[u8]>) -> std::io::Result<()> {
    file.rewind().await?;
    file.write_all(contents.as_ref()).await?;
    file.set_len(contents.as_ref().len() as u64).await?;
    file.sync_all().await?;
    Ok(())
}

pub async fn with_locked_credentials<P, T, F>(config_home: P, f: F) -> Result<T>
where
    P: AsRef<Path>,
    F: for<'a> FnOnce(&'a mut CredentialsFile) -> BoxFuture<'a, Result<T>>,
{
    let path = credentials_path(config_home);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await
        .map_err(|e| {
            error::system(
                &format!(
                    "Failed to open credentials file at {}: {:?}",
                    path.display(),
                    e
                ),
                "",
            )
        })?;
    file.lock_exclusive().map_err(|e| {
        error::system(
            &format!(
                "Failed to lock credentials file at {}: {:?}",
                path.display(),
                e
            ),
            "",
        )
    })?;
    let res = async {
        let mut contents = String::new();
        let _ = file.read_to_string(&mut contents).await.map_err(|e| {
            error::system(
                &format!(
                    "Failed to read credentials file at {}: {:?}",
                    path.display(),
                    e
                ),
                "",
            )
        })?;
        let mut credentials = if contents.is_empty() {
            CredentialsFile {
                credentials: HashMap::new(),
            }
        } else {
            serde_json::from_str(&contents).map_err(|e| {
                error::system(
                    &format!(
                        "Failed to parse credentials file at {}: {:?}",
                        path.display(),
                        e
                    ),
                    "",
                )
            })?
        };
        let original_credentials = credentials.clone();
        let res = f(&mut credentials).await?;
        if credentials != original_credentials {
            replace_file(
                &mut file,
                serde_json::to_vec_pretty(&credentials).map_err(|e| {
                    error::system(&format!("Failed to serialize credentials: {}", e), "")
                })?,
            )
            .await
            .map_err(|e| {
                error::system(
                    &format!(
                        "Failed to write credentials file at {}: {}",
                        path.display(),
                        e
                    ),
                    "",
                )
            })?;
        }
        Ok(res)
    }
    .await;
    file.unlock().map_err(|e| {
        error::system(
            &format!(
                "Failed to unlock credentials file at {}: {:?}",
                path.display(),
                e
            ),
            "",
        )
    })?;
    res
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_refresh.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2RefreshMutation;

pub async fn get_credentials(
    config_home: impl AsRef<Path>,
    url: Url,
) -> Result<Option<Credentials>> {
    let credentials = with_locked_credentials(config_home, |file| {
        async move {
            let credentials = match file.credentials.get(&url).cloned() {
                Some(credentials) => credentials,
                None => return Ok(None),
            };
            let credentials = credentials.refresh(&url).await?;
            if let Some(credentials) = &credentials {
                file.credentials.insert(url.clone(), credentials.clone());
            }
            Ok(credentials)
        }
        .boxed()
    })
    .await?;
    Ok(credentials)
}
