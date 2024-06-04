use crate::credentials::{with_locked_credentials, Credentials};
use crate::{
    commands::GlobalArgs,
    error::{self, Result},
    shutdown::shutdown_signal,
};
use axum::{
    extract::{Query, State},
    response::Html,
    routing::get,
    Router,
};
use base64::prelude::*;
use chrono::{Duration, Utc};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::{future::IntoFuture, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex},
};
use url::Url;

const CLIENT_ID_PREFIX: &str = "localhost-";

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Login {
    #[arg(long, short, help = "Force login without a browser")]
    interactive: bool,
}

fn client_id() -> String {
    let hostname = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    format!("{CLIENT_ID_PREFIX}{hostname}")
}

impl GlobalArgs {
    fn authorize_url(&self, client_id: &str, redirect_uri: &Url, state: &str) -> Result<Url> {
        let mut url = self.aqora_url()?.join("/oauth2/authorize")?;
        url.query_pairs_mut()
            .append_pair("client_id", client_id)
            .append_pair("state", state)
            .append_pair("redirect_uri", redirect_uri.as_ref())
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
                    tracing::error!("Failed to send OAuth callback response!");
                }
            });
        }
    }
}

async fn open_that(url: Url) -> bool {
    tokio::task::spawn_blocking(move || open::that(url.as_str()))
        .await
        .is_ok()
}

async fn get_oauth_code(
    global: &GlobalArgs,
    client_id: &str,
    progress: &ProgressBar,
    interactive: bool,
) -> Result<Option<(Url, String)>> {
    let (tx, rx) = oneshot::channel();
    let state = ServerState::new(tx);

    let router = Router::<ServerState<LoginResponse>>::new()
        .route("/", get(login_callback))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
        error::user(
            &format!("Could not bind to any port for OAuth callback: {e:?}"),
            "Make sure you have permission to bind to a network port",
        )
    })?;
    let port = listener.local_addr()?.port();
    let http = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .into_future();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let session = BASE64_URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>());
    let redirect_uri = Url::parse(&format!("http://localhost:{port}"))?;
    let authorize_url = global.authorize_url(client_id, &redirect_uri, &session)?;

    if !interactive {
        progress.suspend(|| {
            // NOTE: suspending here instead of just `progress.println(...)`
            // because indicatif will drop out of screen characters instead of wrapping
            println!("Please navigate to this url (it should open automatically):");
            println!("{authorize_url}");
            println!(
                "If it does not open automatically, you can instead run the same command like:"
            );
            println!(" => aqora login --interactive");
        });
        progress.set_message("Waiting for browser response...");
    }

    if interactive || !open_that(authorize_url.clone()).await {
        return login_interactive(global, client_id, progress).await;
    }

    let res = tokio::select! {
        state = rx => state?,
        res = http => match res {
            Ok(_) => return Ok(None),
            Err(e) => return Err(error::user("Failed to start OAuth callback server", &format!("{:?}", e))),
        },
    };

    if res.state != session {
        return Err(error::system(
            "OAuth callback returned invalid state",
            "This is a bug, please report it",
        ));
    }

    Ok(Some((redirect_uri, res.code)))
}

fn prompt_line(prompt: Option<impl AsRef<str>>) -> std::io::Result<String> {
    if let Some(prompt) = prompt {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(prompt.as_ref().as_bytes())?;
        stdout.flush()?;
    }

    let stdin = std::io::stdin();
    let mut buf = String::new();
    stdin.lock().read_line(&mut buf)?;
    Ok(buf.trim_end().to_string())
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/login.graphql",
    schema_path = "src/graphql/schema.graphql",
    variables_derives = "Debug",
    response_derives = "Debug"
)]
struct LoginPageUserMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_authorize.graphql",
    schema_path = "src/graphql/schema.graphql",
    variables_derives = "Debug",
    response_derives = "Debug"
)]
struct Oauth2AuthorizePageMutation;

async fn login_interactive(
    global: &GlobalArgs,
    client_id: &str,
    progress: &ProgressBar,
) -> Result<Option<(Url, String)>> {
    if !passterm::isatty(passterm::Stream::Stdout) {
        return Err(error::user(
            "Not in a tty",
            "Please retry in a terminal, and without output redirections",
        ));
    }

    let cloned_progress = progress.clone();
    let (username, password) = tokio::task::spawn_blocking(move || {
        cloned_progress.suspend(|| -> Result<(String, String)> {
            let username = prompt_line(Some("Enter username: ")).map_err(|_| {
                error::system("Could not read username from stdin", "Please retry again")
            })?;
            let password =
                passterm::prompt_password_tty(Some("Enter password: ")).map_err(|_| {
                    error::system("Could not read password from tty", "Please retry again")
                })?;
            Ok((username, password))
        })
    })
    .await
    .map_err(|_| error::user("Interactive login has been cancelled", ""))??;

    let client = reqwest::Client::new();

    progress.set_message("Authenticating [1/2]");
    let access_token = {
        let data = LoginPageUserMutation::build_query(login_page_user_mutation::Variables {
            input: login_page_user_mutation::LoginUserInput {
                username_or_email: username,
                password,
            },
        });
        let response = client
            .post(global.graphql_url()?)
            .json(&data)
            .send()
            .await?;
        let header = response.headers().get("x-access-token").ok_or(error::user(
            "Invalid username or password",
            "Please check your credentials",
        ))?;
        header
            .to_str()
            .map_err(|_| {
                error::system("Invalid data returned by server", "Please try again later")
            })?
            .to_string()
    };

    let redirect_uri: Url = "http://localhost/".parse().unwrap();

    progress.set_message("Authenticating [2/2]");
    let oauth_token = {
        let data =
            Oauth2AuthorizePageMutation::build_query(oauth2_authorize_page_mutation::Variables {
                input: oauth2_authorize_page_mutation::Oauth2AuthorizeInput {
                    client_id: client_id.into(),
                    redirect_uri: Some(redirect_uri.clone()),
                    state: Some(BASE64_URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>())),
                },
            });
        let response = client
            .post(global.graphql_url()?)
            .header("Authorization", format!("Bearer {access_token}"))
            .json(&data)
            .send()
            .await?;
        let data = response
            .json::<graphql_client::Response<
                <Oauth2AuthorizePageMutation as graphql_client::GraphQLQuery>::ResponseData,
            >>()
            .await
            .map_err(|_| {
                error::system("Invalid data returned by server", "Please try again later")
            })?;
        let data = data.data.ok_or(error::system(
            "Invalid data returned by backend",
            "Please try again later",
        ))?;
        if let Some(uri) = data.oauth2_authorize.redirect_uri {
            uri.query_pairs()
                .find_map(|(key, value)| {
                    if key == "code" {
                        Some(value.into_owned())
                    } else {
                        None
                    }
                })
                .ok_or(error::system(
                    "Invalid data returned by backend",
                    "Please retry again",
                ))?
        } else if data.oauth2_authorize.client_error || data.oauth2_authorize.unauthorized {
            return Err(error::user(
                "Server denied this authentication request",
                "Please try again later",
            ));
        } else {
            return Err(error::system(
                "Server could not authenticate your account",
                "Please try again later",
            ));
        }
    };

    progress.set_message("Authenticated!");
    Ok(Some((redirect_uri, oauth_token)))
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_token.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2TokenMutation;

pub async fn login(args: Login, global: GlobalArgs) -> Result<()> {
    with_locked_credentials(|file| {
        async move {
            let progress = ProgressBar::new_spinner().with_message("Logging in...");
            progress.enable_steady_tick(std::time::Duration::from_millis(100));
            let client_id = client_id();
            let (redirect_uri, code) = if let Some(res) =
                get_oauth_code(&global, &client_id, &progress, args.interactive).await?
            {
                res
            } else {
                // cancelled
                return Ok(());
            };
            let client = reqwest::Client::new();
            let result = graphql_client::reqwest::post_graphql::<Oauth2TokenMutation, _>(
                &client,
                global.graphql_url()?,
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
                    expires_at: Utc::now() + Duration::try_seconds(issued.expires_in).unwrap(),
                };
                file.credentials.insert(global.aqora_url()?, credentials);
            } else {
                return Err(error::system(
                    "GraphQL response missing issued",
                    "This is a bug, please report it",
                ));
            }
            progress.finish_with_message("Logged in successfully!");
            Ok(())
        }
        .boxed()
    })
    .await
}
