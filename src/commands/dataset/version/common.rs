use crate::{
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};
use graphql_client::GraphQLQuery;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_version_info.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetVersionInfo;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_dataset_versions.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetDatasetVersions;

pub async fn get_dataset_version(
    client: &GraphQLClient,
    dataset_id: String,
    major: i64,
    minor: i64,
    patch: i64,
) -> Result<Option<dataset_version_info::DatasetVersionInfoNodeOnDatasetVersion>> {
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

pub async fn get_dataset_versions(
    client: &GraphQLClient,
    variables: get_dataset_versions::Variables,
) -> Result<Option<get_dataset_versions::GetDatasetVersionsDatasetBySlug>> {
    Ok(client
        .send::<GetDatasetVersions>(variables)
        .await?
        .dataset_by_slug)
}
