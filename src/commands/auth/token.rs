use crate::{
    commands::GlobalArgs,
    credentials::load_refreshed_credentials,
    dirs::credentials_path,
    error::{self, Result},
    graphql_client::unauthenticated_client,
};
use aqora_client::ClientOptions;
use clap::Args;
use serde::Serialize;
use url::Url;

#[derive(Args, Debug, Serialize)]
#[command(
    author,
    version,
    about = "Print a valid aqora access token on stdout, refreshing if needed"
)]
pub struct Token {
    #[arg(
        long,
        help = "Target a specific aqora API URL instead of the default. Useful when logged into multiple environments."
    )]
    url: Option<String>,
}

pub async fn token(args: Token, global: GlobalArgs) -> Result<()> {
    let url = match args.url.as_deref() {
        Some(url) => Url::parse(url)?,
        None => global.aqora_url()?,
    };
    let path = credentials_path(global.config_home().await?);
    let client = unauthenticated_client(url.clone(), ClientOptions::default())?;
    let loaded = load_refreshed_credentials(&path, &url, &client)
        .await
        .map_err(|e| {
            error::system(
                &format!("Failed to load credentials for {url}: {e}"),
                "Check your configuration and try again.",
            )
        })?;
    let Some((_guard, credentials)) = loaded else {
        return Err(error::user(
            &format!("Not logged in to {url}"),
            "Run 'aqora login' to authenticate.",
        ));
    };
    println!("{}", credentials.access_token);
    Ok(())
}
