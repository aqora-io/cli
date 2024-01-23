mod error;
mod graphql_client;
mod login;
mod upload;

use clap::Parser;
use login::{login, Login};
use std::process::exit;
use upload::{upload, Upload};

#[derive(Parser, Debug)]
#[command(author, version, about)]
enum Cli {
    Login(Login),
    Upload(Upload),
}

#[tokio::main]
async fn main() {
    if let Err(e) = match Cli::parse() {
        Cli::Login(args) => login(args).await,
        Cli::Upload(args) => upload(args).await,
    } {
        eprintln!("{}", e);
        exit(1)
    }
}
