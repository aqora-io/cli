mod convert;
mod infer;
mod upload;
mod utils;

use convert::{convert, Convert};
use infer::{infer, Infer};
use upload::{upload, Upload};

use clap::Subcommand;
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

#[derive(Subcommand, Debug, Serialize)]
pub enum Dataset {
    #[command(hide = true)]
    Infer(Infer),
    #[command(hide = true)]
    Convert(Convert),
    Upload(Upload),
}

pub async fn dataset(args: Dataset, global: GlobalArgs) -> Result<()> {
    match args {
        Dataset::Infer(args) => infer(args, global).await,
        Dataset::Convert(args) => convert(args, global).await,
        Dataset::Upload(args) => upload(args, global).await,
    }
}
