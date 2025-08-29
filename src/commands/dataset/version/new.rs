use crate::{
    commands::{
        dataset::{get_dataset_by_slug, DatasetGlobalArgs},
        GlobalArgs,
    },
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    print,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::{Deserialize, Serialize};

use super::get_dataset_version;

/// Create a new dataset version
#[derive(Args, Debug, Serialize)]
pub struct New {
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

pub async fn new(args: New, dataset_global: DatasetGlobalArgs, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;
    let dataset = get_dataset_by_slug(&client, dataset_global.slug).await?;

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
                    // TODO: ASK
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
            let dataset_version = create_dataset_version(
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
