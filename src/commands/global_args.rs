use crate::{error::Result, graphql_client::graphql_url};
use clap::{Args, ColorChoice};
use std::path::PathBuf;
use url::Url;

/// Aqora respects your privacy and follows https://consoledonottrack.com/ :
/// when $DO_NOT_TRACK environment variable is defined, Aqora will not
/// record any statistics or report any incidents.
#[derive(Args, Debug)]
pub struct GlobalArgs {
    #[arg(
        long,
        default_value = "https://app.aqora.io",
        env = "AQORA_URL",
        global = true,
        hide = true
    )]
    pub url: String,
    #[arg(short, long, default_value = ".", global = true)]
    pub project: PathBuf,
    #[arg(long, global = true)]
    pub uv: Option<PathBuf>,
    #[arg(long, default_value_t = ColorChoice::Auto, global = true)]
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
