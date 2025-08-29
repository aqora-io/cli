mod convert;
mod infer;
mod upload;
mod utils;
mod version;

use convert::{convert, Convert};
use get_dataset_by_slug::GetDatasetBySlugDatasetBySlug;
use infer::{infer, Infer};
use upload::{upload, Upload};
use version::{version, Version};

use clap::{Args, Subcommand};
use serde::Serialize;

use super::GlobalArgs;
use crate::{
    error::{self, Result},
    graphql_client::GraphQLClient,
};
use graphql_client::GraphQLQuery;

#[derive(Args, Debug, Serialize, Clone)]
pub struct DatasetGlobalArgs {
    /// Dataset you want to upload to, must respect "{owner}/{dataset}" form.
    slug: String,
}

#[derive(Subcommand, Debug, Serialize)]
pub enum Dataset {
    #[command(hide = true)]
    Infer(Infer),
    #[command(hide = true)]
    Convert(Convert),
    Upload {
        #[command(flatten)]
        dataset_global: DatasetGlobalArgs,
        #[command(flatten)]
        args: Upload,
    },
    Version {
        #[command(flatten)]
        dataset_global: DatasetGlobalArgs,
        #[command(subcommand)]
        args: Version,
    },
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_dataset_by_slug.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetDatasetBySlug;

pub async fn get_dataset_by_slug(
    client: &GraphQLClient,
    slug: String,
) -> Result<GetDatasetBySlugDatasetBySlug> {
    // Find dataset the user wants to upload
    let Some((owner, local_slug)) = slug.split_once('/') else {
        return Err(error::user(
            "Malformed slug",
            "Expected a slug like: {owner}/{dataset}",
        ));
    };

    let dataset = client
        .send::<GetDatasetBySlug>(get_dataset_by_slug::Variables {
            owner: owner.to_string(),
            local_slug: local_slug.to_string(),
        })
        .await?
        .dataset_by_slug;

    let Some(dataset) = dataset else {
        return Err(error::user("dataset does not exist", "TODO: create it"));
    };

    Ok(dataset)
}

pub async fn dataset(args: Dataset, global: GlobalArgs) -> Result<()> {
    match args {
        Dataset::Infer(args) => infer(args, global).await,
        Dataset::Convert(args) => convert(args, global).await,
        Dataset::Upload {
            dataset_global,
            args,
        } => upload(args, dataset_global, global).await,
        Dataset::Version {
            dataset_global,
            args,
        } => version(args, dataset_global, global).await,
    }
}
