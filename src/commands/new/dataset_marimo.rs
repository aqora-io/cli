use std::path::PathBuf;

use aqora_template::DatasetMarimoTemplate;
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::{self, format_permission_error, Result};
use crate::graphql_client::custom_scalars::*;

#[derive(Args, Debug, Serialize)]
pub struct DatasetMarimo {
    #[arg(short, long)]
    version: Option<String>,
    slug: String,
    dest: Option<PathBuf>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_latest_dataset_version.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetLatestDatasetVersion;

pub async fn dataset_marimo(args: DatasetMarimo, global: GlobalArgs) -> Result<()> {
    let pb = global.spinner().with_message(format!(
        "Creating dataset marimo notebook for '{}'",
        args.slug
    ));

    let (owner, local_slug) = args
        .slug
        .split_once('/')
        .ok_or_else(|| error::user("Malformed slug", "Expected a slug like: {owner}/{dataset}"))?;

    let version = if let Some(version) = args.version {
        version
    } else {
        global
            .graphql_client()
            .await?
            .send::<GetLatestDatasetVersion>(get_latest_dataset_version::Variables {
                owner: owner.to_string(),
                local_slug: local_slug.to_string(),
            })
            .await?
            .dataset_by_slug
            .and_then(|d| d.latest_version)
            .map(|v| v.version)
            .ok_or_else(|| {
                error::user(
                    "No version found",
                    "Please publish a version or specify a draft version",
                )
            })?
    };

    let dest = args.dest.unwrap_or_else(|| PathBuf::from(local_slug));
    DatasetMarimoTemplate::builder()
        .owner(owner)
        .local_slug(local_slug)
        .version(version)
        .render(&dest)
        .map_err(|e| format_permission_error("create use case", &dest, &e))?;

    pb.finish_with_message(format!(
        "Created dataset marimo noteboook in '{}'",
        dest.display()
    ));
    Ok(())
}
