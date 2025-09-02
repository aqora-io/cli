use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::{self, Result};

use super::new::prompt_for_dataset_creation;

#[derive(Args, Debug, Serialize, Clone)]
pub struct DatasetCommonArgs {
    /// Dataset you want to upload to, must respect "{owner}/{dataset}" form.
    pub slug: String,
}

impl DatasetCommonArgs {
    pub fn slug_pair(&self) -> Result<(&str, &str)> {
        self.slug
            .split_once('/')
            .ok_or_else(|| error::user("Malformed slug", "Expected a slug like: {owner}/{dataset}"))
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_dataset_by_slug.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetDatasetBySlug;

pub struct GetDatasetSlugResponse {
    pub id: String,
    pub viewer_can_create_version: bool,
}

pub async fn get_dataset_by_slug(
    global: &GlobalArgs,
    owner: impl AsRef<str>,
    local_slug: impl AsRef<str>,
) -> Result<GetDatasetSlugResponse> {
    let client = global.graphql_client().await?;

    let dataset = client
        .send::<GetDatasetBySlug>(get_dataset_by_slug::Variables {
            owner: owner.as_ref().into(),
            local_slug: local_slug.as_ref().into(),
        })
        .await?
        .dataset_by_slug;

    let Some(dataset) = dataset else {
        let new_dataset = prompt_for_dataset_creation(
            global,
            Some(owner.as_ref().into()),
            Some(local_slug.as_ref().into()),
        )
        .await?;

        return Ok(GetDatasetSlugResponse {
            id: new_dataset.id,
            viewer_can_create_version: new_dataset.viewer_can_create_version,
        });
    };

    Ok(GetDatasetSlugResponse {
        id: dataset.id,
        viewer_can_create_version: dataset.viewer_can_create_version,
    })
}
