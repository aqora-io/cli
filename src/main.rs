mod commands;
mod credentials;
mod error;
mod graphql_client;
mod python;

use clap::Parser;
use commands::*;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(author, version, about)]
enum Cli {
    Login(Login),
    Upload(Upload),
    Test(Test),
}

#[tokio::main]
async fn main() {
    if let Err(e) = match Cli::parse() {
        Cli::Login(args) => login(args).await,
        Cli::Upload(args) => upload(args).await,
        Cli::Test(args) => test(args).await,
    } {
        eprintln!("{}", e);
        exit(1)
    }
}
