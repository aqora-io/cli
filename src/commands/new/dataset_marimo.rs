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
    #[arg(short, long)]
    raw_init: Option<String>,
    #[arg(short, long)]
    slug: Option<String>,
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
    let pb = global
        .spinner()
        .with_message("Creating dataset marimo notebook");

    let mut builder = DatasetMarimoTemplate::builder();

    let dest = if let Some(raw) = args.raw_init {
        builder.raw_init(raw);
        args.dest.ok_or_else(|| {
            error::user(
                "Destination must be provided with raw",
                "Please provide --dest",
            )
        })?
    } else {
        let slug = args.slug.ok_or_else(|| {
            error::user(
                "Slug or raw initializer must be provided",
                "Please provide either --slug or --raw",
            )
        })?;
        let (owner, local_slug) = slug.split_once('/').ok_or_else(|| {
            error::user("Malformed slug", "Expected a slug like: {owner}/{dataset}")
        })?;

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
        builder.owner(owner).local_slug(local_slug).version(version);
        args.dest.unwrap_or_else(|| PathBuf::from(local_slug))
    };

    builder
        .render(&dest)
        .map_err(|e| format_permission_error("create dataset-marimo", &dest, &e))?;

    pb.finish_with_message(format!(
        "Created dataset marimo noteboook in '{}'",
        dest.display()
    ));
    Ok(())
}
