use crate::{
    commands::{
        dataset::{new::prompt_for_dataset_creation, DatasetGlobalArgs},
        GlobalArgs,
    },
    error::{self, Result},
    graphql_client::custom_scalars::*,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;

/// List dataset version from Aqora.io
#[derive(Args, Debug, Serialize)]
pub struct List {
    /// The maximum number of version returned
    #[arg(short, long, default_value_t = 10)]
    limit: usize,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_dataset_versions.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetDatasetVersions;

pub async fn list(args: List, dataset_global: DatasetGlobalArgs, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;

    // Find dataset the user wants to upload
    let Some((owner, local_slug)) = dataset_global.slug.split_once('/') else {
        return Err(error::user(
            "Malformed slug",
            "Expected a slug like: {owner}/{dataset}",
        ));
    };

    let dataset_versions = client
        .send::<GetDatasetVersions>(get_dataset_versions::Variables {
            owner: owner.to_string(),
            local_slug: local_slug.to_string(),
            limit: Some(args.limit as _),
        })
        .await?
        .dataset_by_slug;

    let dataset = match dataset_versions {
        Some(dataset) => dataset,
        None => {
            let dataset = prompt_for_dataset_creation(
                &global,
                Some(owner.to_string()),
                Some(local_slug.to_string()),
            )
            .await?;

            client
                .send::<GetDatasetVersions>(get_dataset_versions::Variables {
                    owner: dataset.owner.username.clone(),
                    local_slug: dataset.local_slug.clone(),
                    limit: Some(args.limit as _),
                })
                .await?
                .dataset_by_slug
                .ok_or_else(|| {
                    error::system(
                        "Could not retrieve versions for the newly created dataset.",
                        "Check the dataset on Aqora.io.",
                    )
                })?
        }
    };

    dataset
        .versions
        .nodes
        .iter()
        .for_each(|version| println!("{}", version.version));

    Ok(())
}
