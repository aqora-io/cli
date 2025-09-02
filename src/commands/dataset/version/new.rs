use crate::{
    commands::{
        dataset::common::{get_dataset_by_slug, DatasetCommonArgs},
        GlobalArgs,
    },
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;

use super::common::get_dataset_version;

/// Create a new dataset version
#[derive(Args, Debug, Serialize)]
pub struct New {
    #[command(flatten)]
    common: DatasetCommonArgs,
    /// A base dataset to create the new version from
    /// all metadata and file from parent version would be into
    /// this new version
    #[arg(short)]
    from: Option<semver::Version>,
    #[arg()]
    version: semver::Version,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_version_create.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetVersionCreate;

pub type CreateDatasetVersionInput = dataset_version_create::CreateDatasetVersionInput;

pub async fn create_dataset_version(
    client: &GraphQLClient,
    dataset_id: String,
    input: Option<CreateDatasetVersionInput>,
) -> Result<String> {
    Ok(client
        .send::<DatasetVersionCreate>(dataset_version_create::Variables { dataset_id, input })
        .await?
        .create_dataset_version
        .id)
}

pub async fn new(args: New, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;
    let (owner, local_slug) = args.common.slug_pair()?;
    let dataset = get_dataset_by_slug(&global, owner, local_slug).await?;

    if !dataset.viewer_can_create_version {
        return Err(error::user(
            "You cannot upload a version for this dataset",
            "Did you type the right slug?",
        ));
    }

    let dataset_id = dataset.id;

    match args.from {
        Some(from) => {
            let dataset_version = get_dataset_version(
                &client,
                dataset_id.clone(),
                from.major as _,
                from.minor as _,
                from.patch as _,
            )
            .await?;
            match dataset_version {
                Some(version) => {
                    let _ = create_dataset_version(
                        &client,
                        dataset_id,
                        Some(CreateDatasetVersionInput {
                            dataset_version_id: Some(version.id),
                            version: Some(args.version.to_string()),
                        }),
                    )
                    .await?;
                }
                None => {
                    if !global
                        .confirm()
                        .with_prompt(
                            "The specified parent version does not exist. Do you want to continue and create a new version from scratch?",
                        )
                        .default(false)
                        .interact()?
                    {
                        return Err(error::user(
                            "Dataset version not found",
                            "Verify the version on Aqora.io.",
                        ));
                    }
                    let _ = create_dataset_version(
                        &client,
                        dataset_id,
                        Some(CreateDatasetVersionInput {
                            dataset_version_id: None,
                            version: Some(args.version.to_string()),
                        }),
                    )
                    .await?;
                }
            }
        }
        None => {
            let _ = create_dataset_version(
                &client,
                dataset_id,
                Some(CreateDatasetVersionInput {
                    dataset_version_id: None,
                    version: Some(args.version.to_string()),
                }),
            )
            .await?;
        }
    }

    Ok(())
}
