mod install;
mod login;
mod python;
mod shell;
mod template;
mod test;
mod upload;
mod version;

use install::{install, Install};
use login::{login, Login};
use python::{python, Python};
use shell::{shell, Shell};
use template::{template, Template};
use test::{test, Test};
use upload::{upload, Upload};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version = version::version(), about)]
pub enum Cli {
    Install(Install),
    Login(Login),
    Python(Python),
    Shell(Shell),
    Test(Test),
    Upload(Upload),
    Template(Template),
}

impl Cli {
    pub async fn run() -> crate::error::Result<()> {
        match Self::parse() {
            Cli::Install(args) => install(args).await,
            Cli::Login(args) => login(args).await,
            Cli::Python(args) => python(args).await,
            Cli::Shell(args) => shell(args).await,
            Cli::Test(args) => test(args).await,
            Cli::Upload(args) => upload(args).await,
            Cli::Template(args) => template(args).await,
        }
    }
}
