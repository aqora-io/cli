mod common;
mod convert;
mod infer;
mod new;
mod upload;
mod utils;
mod version;

use clap::Subcommand;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use convert::{convert, Convert};
use infer::{infer, Infer};
use new::{new, New};
use upload::{upload, Upload};
use version::{version, Version};

#[derive(Subcommand, Debug, Serialize)]
pub enum Dataset {
    #[command(hide = true)]
    Infer(Infer),
    #[command(hide = true)]
    Convert(Convert),
    New(New),
    Upload(Upload),
    Version {
        #[command(subcommand)]
        args: Version,
    },
}

pub async fn dataset(args: Dataset, global: GlobalArgs) -> Result<()> {
    match args {
        Dataset::Infer(args) => infer(args, global).await,
        Dataset::Convert(args) => convert(args, global).await,
        Dataset::New(args) => new(args, global).await,
        Dataset::Upload(args) => upload(args, global).await,
        Dataset::Version { args } => version(args, global).await,
    }
}
