mod dataset;
mod use_case;

use clap::Subcommand;
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

use dataset::{dataset, Dataset};
use use_case::{use_case, UseCase};

#[derive(Subcommand, Debug, Serialize)]
pub enum New {
    Dataset(Dataset),
    UseCase(UseCase),
}

pub async fn new(args: New, global: GlobalArgs) -> Result<()> {
    match args {
        New::Dataset(args) => dataset(args, global).await,
        New::UseCase(args) => use_case(args, global).await,
    }
}
