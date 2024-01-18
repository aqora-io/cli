use crate::error::{self, Result};
use axum::{
    extract::{Query, State},
    response::Html,
    routing::get,
    Router,
};
use chrono::{DateTime, Duration, Utc};
use clap::Args;
use fs4::tokio::AsyncFileExt;
use graphql_client::GraphQLQuery;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    io::SeekFrom,
    sync::Arc,
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::TcpListener,
    signal,
    sync::{oneshot, Mutex},
};
use url::Url;

const EXPIRATION_PADDING_SEC: i64 = 60;
const OAUTH_PORTS: &[u16] = &[63400, 10213, 58518, 33080, 16420];

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Login {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
}

fn graphql_url(url: &Url) -> Result<Url> {
    Ok(url.join("/graphql")?)
}

impl Login {
    fn aqora_url(&self) -> Result<Url> {
        Ok(Url::parse(&self.url)?)
    }

    fn client_id(&self, port: u16) -> String {
        format!("local{port}")
    }

    fn redirect_url(&self, port: u16) -> Result<Url> {
        Ok(Url::parse(&format!("http://localhost:{port}"))?)
    }

    fn graphql_url(&self) -> Result<Url> {
        graphql_url(&self.aqora_url()?)
    }

    fn authorize_url(&self, port: u16, state: &str) -> Result<Url> {
        let mut url = self.aqora_url()?.join("/oauth2/authorize")?;
        url.query_pairs_mut()
            .append_pair("client_id", &self.client_id(port))
            .append_pair("state", state)
            .finish();
        Ok(url)
    }
}

async fn tcp_listen() -> Result<(u16, TcpListener)> {
    for port in OAUTH_PORTS.iter().copied() {
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)).await {
            return Ok((port, listener));
        }
    }
    Err(error::user(
        "Could not bind to any port for OAuth callback",
        &format!("Make sure at least one of ports {OAUTH_PORTS:?} is free"),
    ))
}

#[derive(Deserialize, Debug)]
pub struct LoginResponse {
    code: String,
    state: String,
}

async fn login_callback(
    State(state): State<ServerState<LoginResponse>>,
    Query(response): Query<LoginResponse>,
) -> Html<&'static str> {
    state.send(response).await;
    Html(include_str!("html/login_response.html"))
}

struct ServerState<T> {
    tx: Arc<Mutex<Option<oneshot::Sender<T>>>>,
}

impl<T> Clone for ServerState<T> {
    fn clone(&self) -> Self {
        ServerState {
            tx: self.tx.clone(),
        }
    }
}

impl<T> ServerState<T>
where
    T: Send + 'static,
{
    fn new(tx: oneshot::Sender<T>) -> Self {
        ServerState {
            tx: Arc::new(Mutex::new(Some(tx))),
        }
    }

    async fn send(&self, value: T) {
        if let Some(tx) = self.tx.lock().await.take() {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if tx.send(value).is_err() {
                    println!("Failed to send OAuth callback response!");
                }
            });
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn get_oauth_code(login: &Login) -> Result<Option<(u16, String)>> {
    let (port, listener) = tcp_listen().await?;
    let session = hex::encode(rand::random::<[u8; 16]>());
    let (tx, rx) = oneshot::channel();
    let state = ServerState::new(tx);
    let router = Router::<ServerState<LoginResponse>>::new()
        .route("/", get(login_callback))
        .with_state(state);
    let authorize_url = login.authorize_url(port, &session)?;
    println!("Logging in...");
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        println!("Opening {authorize_url}...");
        if open::that(authorize_url.as_str()).is_err() {
            println!("Failed to open browser, please open {authorize_url} manually");
        }
        println!("Waiting for response...");
    });
    let res = tokio::select! {
        state = rx => state?,
        res = axum::serve(listener, router).with_graceful_shutdown(shutdown_signal()).into_future() => {
            return res.map(|_| None).map_err(|e| {
                error::user("Failed to start OAuth callback server", &format!("{:?}", e))
            });
        }
    };
    if res.state != session {
        return Err(error::system(
            "OAuth callback returned invalid state",
            "This is a bug, please report it",
        ));
    }
    Ok(Some((port, res.code)))
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
struct Credentials {
    client_id: String,
    access_token: String,
    refresh_token: String,
    expires_at: DateTime<Utc>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_token.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2TokenMutation;

async fn config_dir() -> Result<std::path::PathBuf> {
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
    credentials: HashMap<Url, Credentials>,
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

pub async fn write_credentials(file: Arc<Mutex<CredentialsFile>>, login: Login) -> Result<()> {
    let (port, code) = if let Some(res) = get_oauth_code(&login).await? {
        res
    } else {
        return Ok(());
    };
    let client = reqwest::Client::new();
    let result = graphql_client::reqwest::post_graphql::<Oauth2TokenMutation, _>(
        &client,
        login.graphql_url()?,
        oauth2_token_mutation::Variables {
            client_id: login.client_id(port),
            code,
            redirect_uri: login.redirect_url(port)?,
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
    .oauth2_token;
    if let Some(issued) = result.issued {
        let credentials = Credentials {
            client_id: login.client_id(port),
            access_token: issued.access_token,
            refresh_token: issued.refresh_token,
            expires_at: Utc::now() + Duration::seconds(issued.expires_in),
        };
        let mut file = file.lock().await;
        file.credentials.insert(login.aqora_url()?, credentials);
    } else {
        return Err(error::system(
            "GraphQL response missing issued",
            "This is a bug, please report it",
        ));
    }
    println!("Logged in successfully!",);
    if let Ok(path) = credentials_path().await {
        println!("Credentials saved to {}", path.display());
    }
    Ok(())
}

pub async fn login(login: Login) -> Result<()> {
    with_locked_credentials(login, write_credentials).await
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
            client_id: credentials.client_id.clone(),
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
