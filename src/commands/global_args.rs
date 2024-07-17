use crate::{colors::serialize_color_choice, error::Result, graphql_client::graphql_url};
use clap::{Args, ColorChoice};
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
    #[arg(long, default_value_t = *DEFAULT_PARALLELISM, global = true)]
    pub max_concurrency: usize,
    #[arg(long, default_value_t = ColorChoice::Auto, global = true)]
    #[serde(serialize_with = "serialize_color_choice")]
    pub color: ColorChoice,
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
}
