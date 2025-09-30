use crate::{
    colors::ColorChoiceExt,
    dialog::{Confirm, FuzzySelect},
    dirs::{config_home, init_venv, opt_init_venv},
    error::{self, Result},
    graphql_client::{authenticate_client, graphql_url, unauthenticated_client, GraphQLClient},
    progress_bar::default_spinner,
};
use aqora_client::ClientOptions;
use aqora_runner::python::{ColorChoice, LinkMode, PipOptions, PyEnv};
use clap::Args;
use comfy_table::Table;
use indicatif::ProgressBar;
use serde::Serialize;
use std::path::PathBuf;
use url::Url;

lazy_static::lazy_static! {
    static ref DEFAULT_PARALLELISM: usize = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
}

const HELP_HEADING: &str = "Global options";

/// Aqora respects your privacy and follows https://consoledonottrack.com/ :
/// when $DO_NOT_TRACK environment variable is defined, Aqora will not
/// record any statistics or report any incidents.
#[derive(Args, Debug, Serialize, Clone)]
pub struct GlobalArgs {
    #[arg(
        help_heading = HELP_HEADING,
        long,
        default_value = "https://aqora.io",
        env = "AQORA_URL",
        global = true,
        hide = true
    )]
    pub url: String,
    #[arg(
        help_heading = HELP_HEADING,
        long,
        env = "AQORA_ALLOW_INSECURE_HOST",
        global = true,
        hide = true
    )]
    pub allow_insecure_host: bool,
    #[arg(
        help_heading = HELP_HEADING,
        long,
        env = "AQORA_CONFIG_HOME",
        global = true
    )]
    pub config_home: Option<PathBuf>,
    #[arg(
        help_heading = HELP_HEADING,
        short,
        long,
        default_value = ".",
        global = true
    )]
    pub project: PathBuf,
    #[arg(help_heading = HELP_HEADING, long, global = true)]
    pub uv: Option<PathBuf>,
    #[arg(help_heading = HELP_HEADING, long, global = true)]
    pub python: Option<String>,
    #[arg(
        help_heading = HELP_HEADING,
        long,
        global = true,
        default_value = "false"
    )]
    pub ignore_venv_aqora: bool,
    #[arg(
        help_heading = HELP_HEADING,
        long,
        default_value_t = *DEFAULT_PARALLELISM,
        global = true
    )]
    pub max_concurrency: usize,
    #[arg(
        help_heading = HELP_HEADING,
        value_enum,
        long,
        default_value_t = ColorChoice::Auto,
        global = true
    )]
    pub color: ColorChoice,
    #[arg(
        help_heading = HELP_HEADING,
        value_enum,
        long,
        default_value_t = LinkMode::Copy,
        global = true
    )]
    pub dep_link_mode: LinkMode,
    #[arg(
        help_heading = HELP_HEADING,
        short = 'y',
        long = "no-prompt",
        help = "Skip interactive dialogs and automatically confirm",
        default_value_t = false,
        global = true
    )]
    pub no_prompt: bool,
    #[arg(
        help_heading = HELP_HEADING,
        short = 'k',
        long = "no-tick",
        help = "Do not use a steady tick for progress bars",
        default_value_t = false,
        global = true
    )]
    pub no_tick: bool,
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

    pub async fn config_home(&self) -> Result<PathBuf> {
        let path = match &self.config_home {
            Some(path) => path.clone(),
            None => config_home()?,
        };
        if tokio::fs::read_dir(&path).await.is_err() {
            tokio::fs::create_dir_all(&path).await.map_err(|e| {
                error::user(
                    &format!(
                        "Failed to create config directory at {}: {}",
                        path.display(),
                        e
                    ),
                    "Make sure you have the necessary permissions",
                )
            })?;
        }
        Ok(path)
    }

    #[inline]
    fn client_options(&self) -> ClientOptions {
        ClientOptions {
            allow_insecure_host: self.allow_insecure_host,
        }
    }

    #[inline]
    pub fn unauthenticated_graphql_client(&self) -> Result<GraphQLClient> {
        unauthenticated_client(self.aqora_url()?, self.client_options())
    }

    pub async fn graphql_client(&self) -> Result<GraphQLClient> {
        let unauthenticated_client = self.unauthenticated_graphql_client()?;
        match self.config_home().await {
            Ok(config_home) => Ok(authenticate_client(config_home, unauthenticated_client).await?),
            Err(err) => {
                tracing::warn!("Could not access credentials: {}", err.description());
                Ok(unauthenticated_client)
            }
        }
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
            self.no_prompt,
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
            self.no_prompt,
            pb,
        )
        .await
    }

    pub fn confirm(&self) -> Confirm {
        Confirm::new()
            .with_theme(self.color.dialoguer())
            .no_prompt(self.no_prompt)
    }

    pub fn fuzzy_select(&self) -> FuzzySelect {
        FuzzySelect::new()
            .with_theme(self.color.dialoguer())
            .no_prompt(self.no_prompt)
    }

    pub fn spinner(&self) -> ProgressBar {
        default_spinner(!self.no_tick)
    }

    pub fn table(&self) -> Table {
        let mut table = Table::new();
        if matches!(self.color.forced(), ColorChoice::Always) {
            table
                .load_preset(comfy_table::presets::UTF8_FULL)
                .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS);
        }
        table
    }
}
