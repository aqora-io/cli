pub mod common;
pub mod list;
pub mod new;
pub mod reset;

use crate::{commands::GlobalArgs, error::Result};
use clap::Subcommand;
use serde::Serialize;

use list::{list, List};
use new::{new, New};
use reset::{reset, Reset};

#[derive(Subcommand, Debug, Serialize)]
pub enum Version {
    New(New),
    List(List),
    Reset(Reset),
}

pub async fn version(args: Version, global: GlobalArgs) -> Result<()> {
    match args {
        Version::New(args) => new(args, global).await,
        Version::List(args) => list(args, global).await,
        Version::Reset(args) => reset(args, global).await,
    }
}
