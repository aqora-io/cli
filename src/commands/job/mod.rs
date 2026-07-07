mod model;

use clap::Subcommand;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use model::{model, Model};

#[derive(Subcommand, Debug, Serialize)]
pub enum Job {
    /// Download the model payload a provider job ran
    Model(Model),
}

pub async fn job(args: Job, global: GlobalArgs) -> Result<()> {
    match args {
        Job::Model(args) => model(args, global).await,
    }
}
