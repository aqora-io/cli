use crate::{
    commands::{
        dataset::{
            common::{get_dataset_by_slug, DatasetCommonArgs},
            version::common::get_dataset_version,
        },
        GlobalArgs,
    },
    error::{self, Result},
    graphql_client::custom_scalars::*,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/clean_dataset_version_files.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct CleanDatasetVersionFiles;

/// Clean a dataset version
#[derive(Args, Debug, Serialize)]
pub struct Clean {
    #[command(flatten)]
    common: DatasetCommonArgs,
    #[arg(short, long)]
    version: semver::Version,
}

pub async fn clean(args: Clean, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;
    let (owner, local_slug) = args.common.slug_pair()?;
    let dataset = get_dataset_by_slug(&global, owner, local_slug).await?;

    let version = args.version;

    let dataset_version = get_dataset_version(
        &client,
        dataset.id,
        version.major as _,
        version.minor as _,
        version.patch as _,
    )
    .await?
    .ok_or(error::user(
        "Dataset version not found",
        "Verify the version on Aqora.io",
    ))?;

    if !global.confirm()
        .with_prompt(
            "Do you really want to clean this version? The files will be permanently deleted and cannot be recovered."
        )
        .default(false)
        .interact()?
    {
        return Ok(());
    }

    client
        .send::<CleanDatasetVersionFiles>(clean_dataset_version_files::Variables {
            dataset_version_id: dataset_version.id,
        })
        .await?;

    Ok(())
}
