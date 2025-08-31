use crate::commands::GlobalArgs;
use crate::{
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;
use slug::slugify;

/// Create a new dataset
#[derive(Args, Debug, Serialize)]
pub struct New {
    /// The name of the dataset
    name: String,

    /// The owner of the dataset (defaults to the current user if omitted)
    owner: Option<String>,

    /// Whether the dataset should be private (defaults to true)
    #[arg(default_value_t = true)]
    private: bool,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_viewer_related_entities.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetViewerRelatedEntities;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/create_dataset.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct CreateDataset;

pub async fn create_dataset(
    client: &GraphQLClient,
    owner: String,
    name: String,
    private: bool,
) -> Result<create_dataset::CreateDatasetCreateDataset> {
    Ok(client
        .send::<CreateDataset>(create_dataset::Variables {
            owner: Some(owner),
            input: create_dataset::CreateDatasetInput {
                name: name.clone(),
                local_slug: slugify(name),
                private,
                tags: vec![],
            },
        })
        .await?
        .create_dataset)
}

pub async fn ask_dataset_owner(
    global: &GlobalArgs,
    default_owner: Option<String>,
) -> Result<String> {
    let client = global.graphql_client().await?;

    let viewer = client
        .send::<GetViewerRelatedEntities>(get_viewer_related_entities::Variables {
            action: get_viewer_related_entities::Action::CREATE_DATASET,
        })
        .await?
        .viewer;

    let organizations = viewer
        .entities
        .nodes
        .into_iter()
        .filter(|entity| entity.id != viewer.id)
        .collect::<Vec<_>>();

    let choices = organizations
        .iter()
        .map(|org| format!("@{} ({})", org.username, org.display_name))
        .chain(std::iter::once(format!("@{} (Myself)", viewer.username)))
        .rev()
        .collect::<Vec<_>>();

    let default_owner_display = match default_owner {
        Some(ref owner) => owner,
        None => &viewer.username,
    };

    let selection = global
        .fuzzy_select()
        .with_initial_text(default_owner_display)
        .with_prompt(format!(
            "Select a dataset owner (default: {})",
            default_owner_display
        ))
        .items(choices)
        .interact_opt()?;

    Ok(match selection {
        Some(0) => viewer.username,
        Some(index) => organizations[index - 1].username.clone(),
        None => default_owner.unwrap_or(viewer.username),
    })
}

pub async fn prompt_for_dataset_creation(
    global: &GlobalArgs,
    owner: Option<String>,
    local_slug: Option<String>,
) -> Result<create_dataset::CreateDatasetCreateDataset> {
    let client = global.graphql_client().await?;

    if !global
        .confirm()
        .with_prompt("This dataset cannot be found, would like to create it?")
        .default(false)
        .interact()?
    {
        return Err(error::user(
            "Dataset not found",
            "Please double check the slug on Aqora.io",
        ));
    }

    let dataset_owner = ask_dataset_owner(global, owner).await?;

    let mut dataset_name = global.confirm().with_prompt("Enter a name for the dataset");
    if let Some(slug) = local_slug {
        dataset_name = dataset_name.with_initial_text(slug);
    }
    let dataset_name = dataset_name.interact_text()?;

    let dataset_visibility = global
        .confirm()
        .with_prompt("Should this dataset be private? (default: private)")
        .default(true)
        .interact()?;

    create_dataset(&client, dataset_owner, dataset_name, dataset_visibility).await
}

pub async fn new(args: New, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;

    let owner = match args.owner {
        Some(owner) => owner,
        None => ask_dataset_owner(&global, None).await?,
    };

    let _ = create_dataset(&client, owner, args.name, args.private).await?;
    Ok(())
}
