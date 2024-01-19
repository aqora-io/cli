mod error;
mod graphql_client;
mod login;
mod test;

use clap::Parser;
use login::{login, Login};
use std::process::exit;
use test::{test, Test};

#[derive(Parser, Debug)]
#[command(author, version, about)]
enum Cli {
    Login(Login),
    Test(Test),
}

#[tokio::main]
async fn main() {
    if let Err(e) = match Cli::parse() {
        Cli::Login(args) => login(args).await,
        Cli::Test(args) => test(args).await,
    } {
        eprintln!("{}", e);
        exit(1)
    }
}
