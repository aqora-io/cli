mod configure_aws;
mod credentials;

use clap::Subcommand;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use configure_aws::{configure_aws, ConfigureAws};
use credentials::{credentials, Credentials};

#[derive(Subcommand, Debug, Serialize)]
pub enum Store {
    /// Mint short-lived S3 credentials for your aqora storage and print them on stdout
    Credentials(Credentials),
    /// Add a profile to your AWS config that fetches aqora storage credentials on demand
    ConfigureAws(ConfigureAws),
}

pub async fn store(args: Store, global: GlobalArgs) -> Result<()> {
    match args {
        Store::Credentials(args) => credentials(args, global).await,
        Store::ConfigureAws(args) => configure_aws(args, global).await,
    }
}
