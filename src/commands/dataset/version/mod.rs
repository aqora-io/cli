mod list;

use crate::{
    commands::GlobalArgs,
    error::{self, Result},
};
use clap::{Args, Subcommand};
use list::{list, List};
use serde::Serialize;

use super::DatasetGlobalArgs;

#[derive(Subcommand, Debug, Serialize)]
pub enum Version {
    List(List),
}

pub async fn version(
    args: Version,
    dataset_global: DatasetGlobalArgs,
    global: GlobalArgs,
) -> Result<()> {
    match args {
        Version::List(args) => list(args, dataset_global, global).await,
    }
}
