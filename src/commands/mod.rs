mod install;
mod login;
mod python;
mod shell;
mod test;
mod upload;

use install::{install, Install};
use login::{login, Login};
use python::{python, Python};
use shell::{shell, Shell};
use test::{test, Test};
use upload::{upload, Upload};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub enum Cli {
    Login(Login),
    Upload(Upload),
    Test(Test),
    Install(Install),
    Shell(Shell),
    Python(Python),
}

impl Cli {
    pub async fn run() -> crate::error::Result<()> {
        match Self::parse() {
            Cli::Login(args) => login(args).await,
            Cli::Upload(args) => upload(args).await,
            Cli::Test(args) => test(args).await,
            Cli::Install(args) => install(args).await,
            Cli::Shell(args) => shell(args).await,
            Cli::Python(args) => python(args).await,
        }
    }
}
