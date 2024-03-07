mod global_args;
mod install;
mod login;
mod python;
mod shell;
mod template;
mod test;
mod upload;
mod version;

pub use global_args::GlobalArgs;

use install::{install, Install};
use login::{login, Login};
use python::{python, Python};
use shell::{shell, Shell};
use template::{template, Template};
use test::{test, Test};
use upload::{upload, Upload};

use crate::colors::ColorChoiceExt;
use clap::{CommandFactory, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version = version::version(), about)]
pub struct Cli {
    #[command(flatten)]
    global: GlobalArgs,
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
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
        let parsed = Self::parse();
        let global = parsed.global;
        if let Err(err) = global.validate() {
            let mut cmd = Self::command();
            cmd.error(clap::error::ErrorKind::InvalidValue, err).exit();
        }
        global.color.set_override();
        match parsed.commands {
            Commands::Install(args) => install(args, global).await,
            Commands::Login(args) => login(args, global).await,
            Commands::Python(args) => python(args, global).await,
            Commands::Shell(args) => shell(args, global).await,
            Commands::Test(args) => test(args, global).await,
            Commands::Upload(args) => upload(args, global).await,
            Commands::Template(args) => template(args, global).await,
        }
    }
}
