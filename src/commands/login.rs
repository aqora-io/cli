use crate::{
    commands::GlobalArgs,
    credentials::{get_credentials, with_locked_credentials, Credentials},
    error::{self, Result},
    graphql_client::unauthenticated_client,
};
use base64::prelude::*;
use chrono::{Duration, Utc};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use ring::signature::KeyPair;
use serde::Serialize;
use std::io::{BufRead, Write};
use url::Url;

const CLIENT_ID_PREFIX: &str = "localhost-";

#[derive(Args, Default, Debug, Serialize)]
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

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_redirect_subscription.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2RedirectSubscription;

async fn get_oauth_code(
    global: &GlobalArgs,
    client_id: &str,
    progress: &ProgressBar,
) -> Result<Option<(Url, String)>> {
    let rng = ring::rand::SystemRandom::new();
    let keypair = ring::signature::Ed25519KeyPair::from_seed_unchecked(
        &ring::rand::generate::<[u8; 32]>(&rng).unwrap().expose(),
    )
    .unwrap();
    let state_bytes = ring::rand::generate::<[u8; 16]>(&rng).unwrap().expose();

    let state = BASE64_URL_SAFE_NO_PAD.encode(state_bytes);
    let public_key = BASE64_URL_SAFE_NO_PAD.encode(keypair.public_key().as_ref());

    let redirect_uri = Url::parse(&format!("https://aqora.io/oauth2/sub/{public_key}"))?;
    let authorize_url = global.authorize_url(client_id, &redirect_uri, &state)?;

    let signature_bytes = keypair.sign(authorize_url.as_str().as_bytes());
    let signature = BASE64_URL_SAFE_NO_PAD.encode(signature_bytes.as_ref());

    let mut subscription = unauthenticated_client(global.aqora_url()?)?
        .subscribe::<Oauth2RedirectSubscription>(oauth2_redirect_subscription::Variables {
            auth_url: authorize_url.clone(),
            signature,
        })
        .await?;

    let cloned_progress = progress.clone();
    let opener = tokio::spawn(async move {
        cloned_progress.set_message("Opening browser and waiting for response...");

        if open::that(authorize_url.as_str()).is_err()
            || tokio::time::sleep(std::time::Duration::from_secs(5))
                .map(|_| true)
                .await
        {
            let qr_string = qrcode::QrCode::new(authorize_url.as_str())
                .map(|qrcode| {
                    use qrcode::render::unicode;
                    let string = qrcode
                        .render::<unicode::Dense1x2>()
                        .dark_color(unicode::Dense1x2::Light)
                        .light_color(unicode::Dense1x2::Dark)
                        .build();
                    format!("\n{string}\n")
                })
                .unwrap_or_default();
            cloned_progress.set_message(format!(
                r#"Waiting for the browser response...

Please navigate to this url:

{authorize_url}
{qr_string}
If you do not have access to a browser
you can instead run the following command:
`aqora login --interactive`"#
            ))
        }
    });

    let Some(item) = subscription.next().await.transpose()? else {
        return Err(error::system(
            "No response from server",
            "Please retry again",
        ));
    };
    opener.abort();

    Ok(Some((redirect_uri, item.oauth2_redirect.code)))
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
    schema_path = "schema.graphql",
    variables_derives = "Debug",
    response_derives = "Debug"
)]
struct LoginPageUserMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_authorize.graphql",
    schema_path = "schema.graphql",
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
                    scope: None,
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
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct Oauth2TokenMutation;

async fn do_login(args: Login, global: GlobalArgs, progress: ProgressBar) -> Result<()> {
    with_locked_credentials(global.config_home().await?, |file| {
        async move {
            progress.set_message("Logging in...");
            let client_id = client_id();
            let Some((redirect_uri, code)) = (if args.interactive {
                login_interactive(&global, &client_id, &progress).await?
            } else {
                get_oauth_code(&global, &client_id, &progress).await?
            }) else {
                // cancelled
                return Ok(());
            };
            let result = unauthenticated_client(global.aqora_url()?)?
                .send::<Oauth2TokenMutation>(oauth2_token_mutation::Variables {
                    client_id: client_id.clone(),
                    code,
                    redirect_uri,
                })
                .await?
                .oauth2_token;
            if let Some(issued) = result.issued {
                let credentials = Credentials {
                    client_id,
                    client_secret: None,
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

pub async fn login(args: Login, global: GlobalArgs) -> Result<()> {
    let pb = global.spinner();
    do_login(args, global, pb).await
}

pub async fn check_login(global: GlobalArgs, multi_progress: &MultiProgress) -> Result<bool> {
    if get_credentials(global.config_home().await?, global.aqora_url()?)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    let confirmation = multi_progress.suspend(|| {
        global
            .confirm()
            .with_prompt(
                "Your aqora account is not currently connected. Would you like to connect it now?",
            )
            .default(true)
            .no_prompt_value(false)
            .interact()
    })?;
    if confirmation {
        let pb = multi_progress.add(global.spinner());
        do_login(Login::default(), global, pb).await?;
    }
    Ok(confirmation)
}
