mod add;
mod clean;
mod global_args;
mod info;
mod install;
mod lab;
mod login;
mod new;
mod python;
mod remove;
mod shell;
mod template;
mod test;
mod upload;
mod version;

use serde::Serialize;

pub use global_args::GlobalArgs;

use add::{add, Add};
use clean::{clean, Clean};
use info::{info, Info};
use install::{install, Install};
use lab::{lab, Lab};
use login::{login, Login};
use new::{new, New};
use python::{python, Python};
use remove::{remove, Remove};
use shell::{shell, Shell};
use template::{template, Template};
use test::{test, Test};
use upload::{upload, Upload};

use crate::{
    colors::ColorChoiceExt, commands::version::version, revert_file::revert_all,
    shutdown::shutdown_signal,
};
use clap::{CommandFactory, Parser, Subcommand};

#[derive(Parser, Debug, Serialize)]
#[command(author, version = version(), about)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,
    #[command(subcommand)]
    pub commands: Commands,
}

#[derive(Subcommand, Debug, Serialize)]
pub enum Commands {
    Install(Install),
    New {
        #[command(subcommand)]
        args: New,
    },
    Login(Login),
    Python(Python),
    Shell(Shell),
    Test(Test),
    Upload(Upload),
    Template(Template),
    Clean(Clean),
    Add(Add),
    Remove(Remove),
    #[command(hide = true)]
    Info(Info),
    Lab(Lab),
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
                Commands::New { args } => new(args, global).await,
                Commands::Login(args) => login(args, global).await,
                Commands::Python(args) => python(args, global).await,
                Commands::Shell(args) => shell(args, global).await,
                Commands::Test(args) => test(args, global).await,
                Commands::Lab(args) => lab(args, global).await,
                Commands::Upload(args) => upload(args, global).await,
                Commands::Template(args) => template(args, global).await,
                Commands::Clean(args) => clean(args, global).await,
                Commands::Info(args) => info(args, global).await,
                Commands::Add(args) => add(args, global).await,
                Commands::Remove(args) => remove(args, global).await,
            }
        };
        tokio::select! {
            res = run => res,
            _ = shutdown_signal() => {
                tracing::warn!("Exiting!");
                revert_all()?;
                Ok(())
            }
        }
    }

    pub async fn run(self) -> bool {
        let command_context =
            sentry::protocol::Context::Other(std::collections::BTreeMap::from([(
                "args".into(),
                serde_json::to_value(&self).unwrap_or_default(),
            )]));

        let runtime_context = pyo3::Python::with_gil(|py| sentry::protocol::RuntimeContext {
            name: Some("Python".into()),
            version: Some(py.version().into()),
            ..Default::default()
        });

        sentry::configure_scope(move |scope| {
            scope.set_context("command", command_context);
            scope.set_context("python", runtime_context);
        });

        if let Err(run_error) = self.do_run().await {
            if run_error.is_user() {
                tracing::error!(
                    error = &run_error as &dyn std::error::Error,
                    is_user = true,
                    "{run_error}"
                );
            } else {
                tracing::error!(error = &run_error as &dyn std::error::Error, "{run_error}");
            }
            false
        } else {
            true
        }
    }
}
