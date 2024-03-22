mod global_args;
mod info;
mod install;
mod login;
mod python;
mod shell;
mod template;
mod test;
mod upload;
mod version;

pub use global_args::GlobalArgs;

use info::{info, Info};
use install::{install, Install};
use login::{login, Login};
use python::{python, Python};
use shell::{shell, Shell};
use template::{template, Template};
use test::{test, Test};
use upload::{upload, Upload};

use crate::{colors::ColorChoiceExt, revert_file::revert_all, shutdown::shutdown_signal};
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
    #[command(hide = true)]
    Info(Info),
}

impl Cli {
    async fn do_run(self) -> crate::error::Result<()> {
        let global = self.global;
        if let Err(err) = global.validate() {
            let mut cmd = Self::command();
            cmd.error(clap::error::ErrorKind::InvalidValue, err).exit();
        }
        global.color.set_override();
        let run = async move {
            match self.commands {
                Commands::Install(args) => install(args, global).await,
                Commands::Login(args) => login(args, global).await,
                Commands::Python(args) => python(args, global).await,
                Commands::Shell(args) => shell(args, global).await,
                Commands::Test(args) => test(args, global).await,
                Commands::Upload(args) => upload(args, global).await,
                Commands::Template(args) => template(args, global).await,
                Commands::Info(args) => info(args, global).await,
            }
        };
        tokio::select! {
            res = run => res,
            _ = shutdown_signal() => {
                eprintln!("Exiting!");
                revert_all()?;
                Ok(())
            }
        }
    }

    pub fn run(self, py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
        pyo3_asyncio::tokio::run::<_, ()>(py, async move {
            if let Err(e) = self.do_run().await {
                eprintln!("{}", e);
                std::process::exit(1)
            }
            std::process::exit(0);
        })
    }
}
