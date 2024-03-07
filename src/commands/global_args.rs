use crate::{error::Result, graphql_client::graphql_url};
use clap::{Args, ColorChoice};
use std::path::PathBuf;
use url::Url;

#[derive(Args, Debug)]
pub struct GlobalArgs {
    #[arg(short, long, default_value = "https://app.aqora.io", global = true)]
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
