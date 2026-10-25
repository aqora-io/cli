mod add;
mod auth;
mod clean;
mod dataset;
mod global_args;
mod info;
mod install;
mod job;
mod lab;
mod login;
mod model;
mod new;
mod pair;
mod python;
mod remove;
mod shell;
mod store;
mod template;
mod test;
mod upload;
mod version;

use serde::Serialize;

pub use global_args::GlobalArgs;

use add::{add, Add};
use auth::{auth, Auth};
use clean::{clean, Clean};
use dataset::{dataset, Dataset};
use info::{info, Info};
use install::{install, Install};
use job::{job, Job};
use lab::{lab, Lab};
use login::{login, Login};
use model::{model, Model};
use new::{new, New};
use pair::{pair, Pair};
use python::{python, Python};
use remove::{remove, Remove};
use shell::{shell, Shell};
use store::{store, Store};
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
    /// Install the project's dependencies and, for a use case, its latest data
    Install(Install),
    /// Create a new project from a template
    New {
        #[command(subcommand)]
        args: New,
    },
    #[clap(hide = true)]
    Dataset {
        #[command(subcommand)]
        args: Dataset,
    },
    /// Inspect and download provider models
    Model {
        #[command(subcommand)]
        args: Model,
    },
    /// Download payloads from provider jobs
    Job {
        #[command(subcommand)]
        args: Job,
    },
    /// Pair an AI coding agent with a workspace or dataset notebook
    Pair(Pair),
    /// Log in to aqora.io
    Login(Login),
    /// Manage aqora access tokens
    Auth {
        #[command(subcommand)]
        args: Auth,
    },
    /// Run Python inside the project's virtual environment
    Python(Python),
    /// Open a shell with the project's virtual environment activated
    Shell(Shell),
    /// Access your private aqora S3 storage
    Store {
        #[command(subcommand)]
        args: Store,
    },
    /// Run the project's tests locally
    Test(Test),
    /// Upload the project to aqora.io
    Upload(Upload),
    /// Download a competition's starter template
    Template(Template),
    /// Remove caches and build artifacts from the project
    Clean(Clean),
    /// Add dependencies to the project
    Add(Add),
    /// Remove dependencies from the project
    Remove(Remove),
    #[command(hide = true)]
    Info(Info),
    /// Open the project in VS Code, or in Jupyter with --jupyter-notebook
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
                Commands::Dataset { args } => dataset(args, global).await,
                Commands::Model { args } => model(args, global).await,
                Commands::Job { args } => job(args, global).await,
                Commands::Pair(args) => pair(args, global).await,
                Commands::Login(args) => login(args, global).await,
                Commands::Auth { args } => auth(args, global).await,
                Commands::Python(args) => python(args, global).await,
                Commands::Shell(args) => shell(args, global).await,
                Commands::Store { args } => store(args, global).await,
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

        let runtime_context = pyo3::Python::attach(|py| sentry::protocol::RuntimeContext {
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

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::CommandFactory;

    fn has_text(text: Option<&clap::builder::StyledStr>) -> bool {
        text.is_some_and(|text| !text.to_string().trim().is_empty())
    }

    fn collect_undocumented(cmd: &clap::Command, path: &str, missing: &mut Vec<String>) {
        for arg in cmd.get_arguments() {
            if arg.is_hide_set() {
                continue;
            }
            if !has_text(arg.get_help()) && !has_text(arg.get_long_help()) {
                missing.push(format!("{path}: argument `{}`", arg.get_id()));
            }
        }
        for sub in cmd.get_subcommands() {
            if sub.is_hide_set() {
                continue;
            }
            let sub_path = format!("{path} {}", sub.get_name());
            if !has_text(sub.get_about()) && !has_text(sub.get_long_about()) {
                missing.push(format!("{sub_path}: command description"));
            }
            collect_undocumented(sub, &sub_path, missing);
        }
    }

    /// `aqora help` should describe every visible command and option.
    #[test]
    fn every_visible_command_and_argument_has_help() {
        // `Cli`'s version string reports the embedded interpreter's version.
        pyo3::Python::initialize();
        let mut cmd = Cli::command();
        cmd.build();
        let mut missing = Vec::new();
        collect_undocumented(&cmd, "aqora", &mut missing);
        assert!(
            missing.is_empty(),
            "missing help text:\n{}",
            missing.join("\n")
        );
    }

    #[test]
    fn cli_definition_is_valid() {
        pyo3::Python::initialize();
        Cli::command().debug_assert();
    }
}
