mod convert;
mod infer;
mod utils;

use clap::Subcommand;
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

use convert::{convert, Convert};
use infer::{infer, Infer};

#[derive(Subcommand, Debug, Serialize)]
pub enum Data {
    Infer(Infer),
    Convert(Convert),
}

pub async fn data(args: Data, global: GlobalArgs) -> Result<()> {
    match args {
        Data::Infer(args) => infer(args, global).await,
        Data::Convert(args) => convert(args, global).await,
    }
}
