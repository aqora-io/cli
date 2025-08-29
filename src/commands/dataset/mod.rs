mod convert;
mod infer;
mod upload;
mod utils;
mod version;

use convert::{convert, Convert};
use infer::{infer, Infer};
use upload::{upload, Upload};
use version::{version, Version};

use clap::{Args, Subcommand};
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

#[derive(Args, Debug, Serialize, Clone)]
pub struct DatasetGlobalArgs {
    /// Dataset you want to upload to, must respect "{owner}/{dataset}" form.
    slug: String,
}

#[derive(Subcommand, Debug, Serialize)]
pub enum Dataset {
    #[command(hide = true)]
    Infer(Infer),
    #[command(hide = true)]
    Convert(Convert),
    Upload(Upload),
    Version {
        #[command(flatten)]
        dataset_global: DatasetGlobalArgs,
        #[command(subcommand)]
        args: Version,
    },
}

pub async fn dataset(args: Dataset, global: GlobalArgs) -> Result<()> {
    match args {
        Dataset::Infer(args) => infer(args, global).await,
        Dataset::Convert(args) => convert(args, global).await,
        Dataset::Upload(args) => upload(args, global).await,
        Dataset::Version {
            dataset_global,
            args,
        } => version(args, dataset_global, global).await,
    }
}
