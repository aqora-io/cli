mod list;
mod new;

use super::DatasetGlobalArgs;
use crate::{
    commands::GlobalArgs,
    error::{self, Result},
    graphql_client::GraphQLClient,
};
use clap::{Args, Subcommand};
use dataset_version_info::DatasetVersionInfoNodeOnDatasetVersion;
use graphql_client::GraphQLQuery;
use list::{list, List};
use new::{new, New};
use serde::Serialize;

pub use new::{create_dataset_version, CreateDatasetVersionInput};

#[derive(Subcommand, Debug, Serialize)]
pub enum Version {
    New(New),
    List(List),
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_version_info.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetVersionInfo;

pub async fn get_dataset_version(
    client: &GraphQLClient,
    dataset_id: String,
    major: i64,
    minor: i64,
    patch: i64,
) -> Result<Option<DatasetVersionInfoNodeOnDatasetVersion>> {
    let node = client
        .send::<DatasetVersionInfo>(dataset_version_info::Variables {
            dataset_id: dataset_id.clone(),
            major,
            minor,
            patch,
        })
        .await?
        .node;
    let dataset_version = match node {
        dataset_version_info::DatasetVersionInfoNode::Dataset(dataset) => dataset,
        _ => {
            return Err(error::system(
                "Cannot find dataset by id",
                "This should not happen, kindly report this bug to Aqora developers please!",
            ))
        }
    };
    Ok(dataset_version.version)
}

pub async fn version(
    args: Version,
    dataset_global: DatasetGlobalArgs,
    global: GlobalArgs,
) -> Result<()> {
    match args {
        Version::New(args) => new(args, dataset_global, global).await,
        Version::List(args) => list(args, dataset_global, global).await,
    }
}
