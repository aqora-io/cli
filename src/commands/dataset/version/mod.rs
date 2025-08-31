pub mod common;
pub mod list;
pub mod new;

use crate::{commands::GlobalArgs, error::Result};
use clap::Subcommand;
use serde::Serialize;

use list::{list, List};
use new::{new, New};

#[derive(Subcommand, Debug, Serialize)]
pub enum Version {
    New(New),
    List(List),
}

pub async fn version(args: Version, global: GlobalArgs) -> Result<()> {
    match args {
        Version::New(args) => new(args, global).await,
        Version::List(args) => list(args, global).await,
    }
}
