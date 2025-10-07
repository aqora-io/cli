use std::path::PathBuf;

use aqora_template::DatasetMarimoTemplate;
use clap::Args;
use graphql_client::GraphQLQuery;
use regex::Regex;
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
    #[arg(short, long)]
    name: Option<String>,
    dest: Option<PathBuf>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_latest_dataset_version_by_slug.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetLatestDatasetVersionBySlug;

fn slugify(string: &str) -> String {
    lazy_static::lazy_static! {
        static ref SLUG_REGEX: Regex = Regex::new(r"[^-a-zA-Z0-9_]").unwrap();
    }
    SLUG_REGEX.replace_all(string, "_").to_lowercase()
}

pub async fn dataset_marimo(args: DatasetMarimo, global: GlobalArgs) -> Result<()> {
    let pb = global
        .spinner()
        .with_message("Creating dataset marimo notebook");

    let mut builder = DatasetMarimoTemplate::builder();

    let slug = args
        .slug
        .as_ref()
        .map(|slug| {
            slug.split_once('/').ok_or_else(|| {
                error::user("Malformed slug", "Expected a slug like: {owner}/{dataset}")
            })
        })
        .transpose()?;

    let dest = if let Some(name) = args.name.as_ref() {
        builder.name(name);
        args.dest.unwrap_or_else(|| PathBuf::from(name))
    } else if let Some((_, local_slug)) = slug {
        builder.name(local_slug);
        args.dest.unwrap_or_else(|| PathBuf::from(local_slug))
    } else if let Some(dest) = args.dest {
        let file_name = if let Some(file_name) = dest.file_name() {
            file_name.to_string_lossy().to_string()
        } else {
            std::env::current_dir()?
                .file_name()
                .ok_or_else(|| error::user("Name not provided", "Please provide a name"))?
                .to_string_lossy()
                .to_string()
        };
        builder.name(slugify(&file_name));
        dest
    } else {
        return Err(error::user(
            "Name or destination must be provided",
            "Please provide either --slug, --name or destination",
        ));
    };

    if let Some(raw) = args.raw_init {
        builder.raw_init(raw);
    } else if let Some((owner, local_slug)) = slug {
        let version = if let Some(version) = args.version {
            version
        } else {
            global
                .graphql_client()
                .await?
                .send::<GetLatestDatasetVersionBySlug>(
                    get_latest_dataset_version_by_slug::Variables {
                        owner: owner.to_string(),
                        local_slug: local_slug.to_string(),
                    },
                )
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
    } else {
        return Err(error::user(
            "Slug or raw initializer must be provided",
            "Please provide either --slug or --raw",
        ));
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
