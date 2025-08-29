use crate::{
    commands::{
        dataset::{upload::prompt_dataset_creation, DatasetGlobalArgs},
        GlobalArgs,
    },
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};
use aqora_client::{
    checksum::{crc32fast::Crc32, S3ChecksumLayer},
    retry::{BackoffRetryLayer, ExponentialBackoffBuilder, RetryStatusCodeRange},
};
use aqora_data_utils::{aqora_client::DatasetVersionFileUploader, infer, read, write, Schema};
use clap::Args;
use futures::{StreamExt as _, TryStreamExt as _};
use graphql_client::GraphQLQuery;
use serde::Serialize;
use std::{fmt::Display, path::PathBuf, str::FromStr};
use thiserror::Error;

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
        .await?;

    let Some(dataset_versions) = dataset_versions.dataset_by_slug else {
        return prompt_dataset_creation(global).await;
    };

    dataset_versions
        .versions
        .nodes
        .iter()
        .for_each(|version| println!("{}", version.version));

    Ok(())
}
