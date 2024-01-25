use crate::{
    error::{self, Result},
    graphql_client::graphql_url,
};
use chrono::{DateTime, Duration, Utc};
use fs4::tokio::AsyncFileExt;
use graphql_client::GraphQLQuery;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::Future, io::SeekFrom, sync::Arc};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};
use url::Url;

const EXPIRATION_PADDING_SEC: i64 = 60;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Credentials {
    pub client_id: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_token.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2TokenMutation;

pub async fn config_dir() -> Result<std::path::PathBuf> {
    let mut path = dirs::data_dir().or_else(dirs::config_dir).ok_or_else(|| {
        error::system(
            "Could not find config directory",
            "This is a bug, please report it",
        )
    })?;
    path.push("aqora");
    tokio::fs::create_dir_all(&path).await.map_err(|e| {
        error::system(
            &format!(
                "Failed to create config directory at {}: {:?}",
                path.display(),
                e
            ),
            "",
        )
    })?;
    Ok(path)
}

pub async fn credentials_path() -> Result<std::path::PathBuf> {
    Ok(config_dir().await?.join("credentials.json"))
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub struct CredentialsFile {
    pub credentials: HashMap<Url, Credentials>,
}

pub async fn with_locked_credentials<T, U, F, Fut>(args: T, f: F) -> Result<U>
where
    F: FnOnce(Arc<Mutex<CredentialsFile>>, T) -> Fut,
    Fut: Future<Output = Result<U>>,
{
    let path = credentials_path().await?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
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
        let credentials = if contents.is_empty() {
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
        let credentials = Arc::new(Mutex::new(credentials));
        let res = f(credentials.clone(), args).await?;
        let credentials = credentials.lock().await;
        if *credentials != original_credentials {
            file.set_len(0).await?;
            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(
                serde_json::to_vec_pretty(&*credentials)
                    .map_err(|e| {
                        error::system(&format!("Failed to serialize credentials: {}", e), "")
                    })?
                    .as_slice(),
            )
            .await
            .map_err(|e| {
                error::system(
                    &format!(
                        "Failed to write credentials file at {}: {:?}",
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
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2RefreshMutation;

async fn read_credentials(
    file: Arc<Mutex<CredentialsFile>>,
    url: Url,
) -> Result<Option<Credentials>> {
    let credentials = match file.lock().await.credentials.get(&url).cloned() {
        Some(credentials) => credentials,
        None => return Ok(None),
    };
    if (credentials.expires_at - Duration::seconds(EXPIRATION_PADDING_SEC)) > Utc::now() {
        return Ok(Some(credentials));
    }
    let client = reqwest::Client::new();
    let result = graphql_client::reqwest::post_graphql::<Oauth2RefreshMutation, _>(
        &client,
        graphql_url(&url)?,
        oauth2_refresh_mutation::Variables {
            client_id: credentials.client_id.clone(),
            refresh_token: credentials.refresh_token,
        },
    )
    .await?
    .data
    .ok_or_else(|| {
        error::system(
            "GraphQL response missing data",
            "This is a bug, please report it",
        )
    })?
    .oauth2_refresh;
    let credentials = if let Some(issued) = result.issued {
        let credentials = Credentials {
            client_id: credentials.client_id,
            access_token: issued.access_token,
            refresh_token: issued.refresh_token,
            expires_at: Utc::now() + Duration::seconds(issued.expires_in),
        };
        let mut file = file.lock().await;
        file.credentials.insert(url, credentials.clone());
        credentials
    } else {
        return Err(error::system(
            "GraphQL response missing issued",
            "This is a bug, please report it",
        ));
    };
    Ok(Some(credentials))
}

pub async fn get_access_token(url: &Url) -> Result<Option<String>> {
    let credentials = with_locked_credentials(url.clone(), read_credentials).await?;
    Ok(credentials.map(|credentials| credentials.access_token))
}
