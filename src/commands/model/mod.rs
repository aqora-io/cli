mod common;
mod download;
mod info;

use clap::Subcommand;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use download::{download, Download};
use info::{info, Info};

#[derive(Subcommand, Debug, Serialize)]
pub enum Model {
    /// Show a provider model's id and creation time
    Info(Info),
    /// Download a provider model's uploaded payload
    Download(Download),
}

pub async fn model(args: Model, global: GlobalArgs) -> Result<()> {
    match args {
        Model::Info(args) => info(args, global).await,
        Model::Download(args) => download(args, global).await,
    }
}
