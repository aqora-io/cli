mod dataset_marimo;
mod use_case;

use clap::Subcommand;
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

use dataset_marimo::{dataset_marimo, DatasetMarimo};
use use_case::{use_case, UseCase};

#[derive(Subcommand, Debug, Serialize)]
pub enum New {
    UseCase(UseCase),
    #[clap(hide = true)]
    DatasetMarimo(DatasetMarimo),
}

pub async fn new(args: New, global: GlobalArgs) -> Result<()> {
    match args {
        New::UseCase(args) => use_case(args, global).await,
        New::DatasetMarimo(args) => dataset_marimo(args, global).await,
    }
}
