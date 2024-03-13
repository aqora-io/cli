use crate::{
    commands::GlobalArgs,
    download::download_tar_gz,
    error::{self, Result},
    graphql_client::GraphQLClient,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use std::path::PathBuf;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_template.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct GetCompetitionTemplate;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Template {
    pub competition: String,
    pub destination: Option<PathBuf>,
}

pub async fn template(args: Template, global: GlobalArgs) -> Result<()> {
    let client = GraphQLClient::new(global.url.parse()?).await?;

    let destination = args
        .destination
        .unwrap_or_else(|| PathBuf::from(args.competition.clone()));

    if destination.exists()
        && (destination.is_file()
            || destination.is_symlink()
            || tokio::fs::read_dir(&destination)
                .await?
                .next_entry()
                .await?
                .is_some())
    {
        return Err(error::user(
            &format!("Destination '{}' already exists", destination.display()),
            "Please specify a different destination",
        ));
    }

    let m = MultiProgress::new();
    let mut pb = ProgressBar::new_spinner().with_message("Fetching competition...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb = m.add(pb);

    let use_case = client
        .send::<GetCompetitionTemplate>(get_competition_template::Variables {
            slug: args.competition.clone(),
        })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{}' not found", &args.competition),
                "Please make sure the competition exists",
            )
        })?
        .use_case
        .latest
        .ok_or_else(|| {
            error::system(
                "No use case found",
                "Please contact the competition organizer",
            )
        })?;

    let download_url = use_case
        .files
        .into_iter()
        .find(|file| {
            matches!(
                file.kind,
                get_competition_template::ProjectVersionFileKind::TEMPLATE
            )
        })
        .ok_or_else(|| {
            error::system(
                "No template found",
                "Please contact the competition organizer",
            )
        })?
        .download_url;

    pb.set_message("Downloading competition template...");

    download_tar_gz(download_url, &destination).await?;

    pb.finish_with_message(format!(
        "Competition template downloaded to {}",
        destination.display()
    ));

    Ok(())
}
