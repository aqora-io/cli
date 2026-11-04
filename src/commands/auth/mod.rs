mod token;

use clap::Subcommand;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use token::{token, Token};

#[derive(Subcommand, Debug, Serialize)]
pub enum Auth {
    /// Print a valid aqora access token on stdout, refreshing if needed
    Token(Token),
}

pub async fn auth(args: Auth, global: GlobalArgs) -> Result<()> {
    match args {
        Auth::Token(args) => token(args, global).await,
    }
}
