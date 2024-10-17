use std::path::PathBuf;

use aqora_template::UseCaseTemplate;
use clap::Args;
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use serde::Serialize;

use crate::error::{self, Result};
use crate::graphql_client::{custom_scalars::*, GraphQLClient};

use super::GlobalArgs;

#[derive(Args, Debug, Serialize)]
pub struct UseCase {
    competition: String,
    dest: Option<PathBuf>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/use_case_template_info.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
struct UseCaseTemplateInfo;

pub async fn use_case(args: UseCase, global: GlobalArgs) -> Result<()> {
    let pb = ProgressBar::new_spinner()
        .with_message(format!("Creating use case for '{}'", args.competition));
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let client = GraphQLClient::new(global.url.parse()?).await?;
    let competition = client
        .send::<UseCaseTemplateInfo>(use_case_template_info::Variables {
            slug: args.competition.clone(),
        })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{}' not found", args.competition),
                "Please make sure the competition is correct",
            )
        })?;
    if competition.use_case.latest.is_some() {
        tracing::warn!("There already exists a use case for this competition. We currently do not copy the use case source code, but will in the future");
    }
    let dest = args
        .dest
        .unwrap_or_else(|| PathBuf::from(&args.competition));
    UseCaseTemplate::builder()
        .competition(args.competition)
        .title(competition.title)
        .render(&dest)
        .map_err(|e| {
            error::user(
                &format!("Failed to create use case at '{}': {}", dest.display(), e),
                &format!(
                    "Make sure you have the correct permissions for '{}'",
                    dest.display()
                ),
            )
        })?;
    pb.finish_with_message(format!(
        "Created use case in directory '{}'",
        dest.display()
    ));
    Ok(())
}
