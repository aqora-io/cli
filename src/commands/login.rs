use crate::credentials::{with_locked_credentials, Credentials, CredentialsFile};
use crate::{
    error::{self, Result},
    graphql_client::graphql_url,
};
use axum::{
    extract::{Query, State},
    response::Html,
    routing::get,
    Router,
};
use chrono::{Duration, Utc};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Deserialize;
use std::{future::IntoFuture, sync::Arc};
use tokio::{
    net::TcpListener,
    signal,
    sync::{oneshot, Mutex},
};
use url::Url;

const CLIENT_ID_PREFIX: &str = "localhost-";

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Login {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
}

fn client_id() -> String {
    let hostname = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    format!("{CLIENT_ID_PREFIX}{hostname}")
}

impl Login {
    fn aqora_url(&self) -> Result<Url> {
        Ok(Url::parse(&self.url)?)
    }

    fn graphql_url(&self) -> Result<Url> {
        graphql_url(&self.aqora_url()?)
    }

    fn authorize_url(&self, client_id: &str, redirect_uri: &Url, state: &str) -> Result<Url> {
        let mut url = self.aqora_url()?.join("/oauth2/authorize")?;
        url.query_pairs_mut()
            .append_pair("client_id", client_id)
            .append_pair("state", state)
            .append_pair("redirect_uri", &redirect_uri.to_string())
            .finish();
        Ok(url)
    }
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
    Html(include_str!("../html/login_response.html"))
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

async fn get_oauth_code(login: &Login, client_id: &str) -> Result<Option<(Url, String)>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
        error::user(
            &format!("Could not bind to any port for OAuth callback: {e:?}"),
            "Make sure you have permission to bind to a network port",
        )
    })?;
    let port = listener.local_addr()?.port();
    let redirect_uri = Url::parse(&format!("http://localhost:{port}"))?;
    let session = hex::encode(rand::random::<[u8; 16]>());
    let (tx, rx) = oneshot::channel();
    let state = ServerState::new(tx);
    let router = Router::<ServerState<LoginResponse>>::new()
        .route("/", get(login_callback))
        .with_state(state);
    let authorize_url = login.authorize_url(client_id, &redirect_uri, &session)?;
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
    Ok(Some((redirect_uri, res.code)))
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

pub async fn write_credentials(file: Arc<Mutex<CredentialsFile>>, login: Login) -> Result<()> {
    let client_id = client_id();
    let (redirect_uri, code) = if let Some(res) = get_oauth_code(&login, &client_id).await? {
        res
    } else {
        // cancelled
        return Ok(());
    };
    let client = reqwest::Client::new();
    let result = graphql_client::reqwest::post_graphql::<Oauth2TokenMutation, _>(
        &client,
        login.graphql_url()?,
        oauth2_token_mutation::Variables {
            client_id: client_id.clone(),
            code,
            redirect_uri,
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
            client_id,
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
