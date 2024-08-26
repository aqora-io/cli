use crate::{
    colors::ColorChoiceExt,
    dirs::{init_venv, opt_init_venv},
    error::Result,
    graphql_client::graphql_url,
};
use aqora_runner::python::{ColorChoice, LinkMode, PipOptions, PyEnv};
use clap::Args;
use indicatif::ProgressBar;
use serde::Serialize;
use std::path::PathBuf;
use url::Url;

lazy_static::lazy_static! {
    static ref DEFAULT_PARALLELISM: usize = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
}

/// Aqora respects your privacy and follows https://consoledonottrack.com/ :
/// when $DO_NOT_TRACK environment variable is defined, Aqora will not
/// record any statistics or report any incidents.
#[derive(Args, Debug, Serialize)]
pub struct GlobalArgs {
    #[arg(
        long,
        default_value = "https://aqora.io",
        env = "AQORA_URL",
        global = true,
        hide = true
    )]
    pub url: String,
    #[arg(short, long, default_value = ".", global = true)]
    pub project: PathBuf,
    #[arg(long, global = true)]
    pub uv: Option<PathBuf>,
    #[arg(long, global = true)]
    pub python: Option<String>,
    #[arg(long, global = true, default_value = "false")]
    pub ignore_venv_aqora: bool,
    #[arg(long, default_value_t = *DEFAULT_PARALLELISM, global = true)]
    pub max_concurrency: usize,
    #[arg(value_enum, long, default_value_t = ColorChoice::Auto, global = true)]
    pub color: ColorChoice,
    #[arg(value_enum, long, default_value_t = LinkMode::Copy, global = true)]
    pub dep_link_mode: LinkMode,
}

impl GlobalArgs {
    pub fn validate(&self) -> Result<(), String> {
        if let Err(err) = Url::parse(&self.url) {
            return Err(format!("Invalid url: {}", err));
        }
        Ok(())
    }

    pub fn aqora_url(&self) -> Result<Url> {
        Ok(Url::parse(&self.url)?)
    }

    pub fn graphql_url(&self) -> Result<Url> {
        graphql_url(&self.aqora_url()?)
    }

    pub fn pip_options(&self) -> PipOptions {
        PipOptions {
            color: self.color.forced(),
            link_mode: self.dep_link_mode,
            ..Default::default()
        }
    }

    pub async fn init_venv(&self, pb: &ProgressBar) -> Result<PyEnv> {
        init_venv(
            &self.project,
            self.uv.as_ref(),
            self.python.as_ref(),
            self.color.forced(),
            self.dep_link_mode,
            pb,
        )
        .await
    }

    pub async fn opt_init_venv(&self, pb: &ProgressBar) -> Result<Option<PyEnv>> {
        opt_init_venv(
            &self.project,
            self.uv.as_ref(),
            self.python.as_ref(),
            self.color.forced(),
            self.dep_link_mode,
            pb,
        )
        .await
    }
}
