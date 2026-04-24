mod token;

use clap::Subcommand;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use token::{token, Token};

#[derive(Subcommand, Debug, Serialize)]
pub enum Auth {
    Token(Token),
}

pub async fn auth(args: Auth, global: GlobalArgs) -> Result<()> {
    match args {
        Auth::Token(args) => token(args, global).await,
    }
}
