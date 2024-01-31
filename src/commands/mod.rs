mod login;
mod test;
mod upload;

use login::{login, Login};
use test::{test, Test};
use upload::{upload, Upload};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub enum Cli {
    Login(Login),
    Upload(Upload),
    Test(Test),
}

impl Cli {
    pub async fn run() -> crate::error::Result<()> {
        match Self::parse() {
            Cli::Login(args) => login(args).await,
            Cli::Upload(args) => upload(args).await,
            Cli::Test(args) => test(args).await,
        }
    }
}
