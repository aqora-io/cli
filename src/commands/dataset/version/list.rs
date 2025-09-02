use crate::{
    commands::{
        dataset::{
            common::DatasetCommonArgs, new::prompt_for_dataset_creation,
            version::common::get_dataset_versions,
        },
        GlobalArgs,
    },
    error::{self, Result},
};
use clap::Args;
use comfy_table::Table;
use serde::Serialize;

/// List dataset version from Aqora.io
#[derive(Args, Debug, Serialize)]
pub struct List {
    #[command(flatten)]
    common: DatasetCommonArgs,
    /// The maximum number of version returned
    #[arg(short, long, default_value_t = 10)]
    limit: usize,
}

fn versions_to_table(
    global: &GlobalArgs,
    versions: impl IntoIterator<
        Item = get_dataset_versions::GetDatasetVersionsDatasetBySlugVersionsNodes,
    >,
) -> Table {
    let mut table = global.table();
    table.set_header(vec!["Version", "Published At", "Updated At", "Size"]);

    let rows = versions.into_iter().map(|version| {
        let published_at = version
            .published_at
            .as_ref()
            .map(|ts| ts.format("%d/%m/%Y %H:%M").to_string())
            .unwrap_or_else(|| "— not published —".to_string());

        vec![
            version.version.clone(),
            published_at,
            version.updated_at.format("%d/%m/%Y %H:%M").to_string(),
            format!("{}B", version.size),
        ]
    });

    table.add_rows(rows);
    table
}

pub async fn list(args: List, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;

    // Find dataset the user wants to upload
    let (owner, local_slug) = args.common.slug_pair()?;

    let dataset_versions = get_dataset_versions(
        &client,
        get_dataset_versions::Variables {
            owner: owner.to_string(),
            local_slug: local_slug.to_string(),
            limit: Some(args.limit as _),
            filters: None,
        },
    )
    .await?;

    let dataset = match dataset_versions {
        Some(dataset) => dataset,
        None => {
            prompt_for_dataset_creation(
                &global,
                Some(owner.to_string()),
                Some(local_slug.to_string()),
            )
            .await?;

            get_dataset_versions(
                &client,
                get_dataset_versions::Variables {
                    owner: owner.to_string(),
                    local_slug: local_slug.to_string(),
                    limit: Some(args.limit as _),
                    filters: None,
                },
            )
            .await?
            .ok_or_else(|| {
                error::system(
                    "Could not retrieve versions for the newly created dataset.",
                    "Check the dataset on Aqora.io.",
                )
            })?
        }
    };

    if dataset.versions.nodes.is_empty() {
        println!("This dataset doesn't have any versions yet.");
        return Ok(());
    }

    let table = versions_to_table(&global, dataset.versions.nodes);
    println!("{table}");

    Ok(())
}
