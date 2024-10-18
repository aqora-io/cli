use crate::{
    commands::{
        install::{install, Install},
        GlobalArgs,
    },
    download::download_archive,
    error::{self, Result},
    graphql_client::GraphQLClient,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use owo_colors::OwoColorize;
use serde::Serialize;
use std::path::PathBuf;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_template.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct GetCompetitionTemplate;

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Template {
    #[arg(long)]
    pub no_install: bool,
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
    match download_archive(download_url, &destination, &pb).await {
        Ok(_) => pb.finish_with_message(format!(
            "Competition template downloaded to {}",
            destination.display()
        )),
        Err(error) => {
            pb.finish_with_message("Failed to download competition template");
            return Err(error);
        }
    }

    if !args.no_install {
        let install_global = GlobalArgs {
            project: destination.clone(),
            ..global
        };
        install(
            Install {
                competition: Some(args.competition),
                ..Default::default()
            },
            install_global,
        )
        .await?;
        // Repeat succcess message after install
        println!(
            "\n{} Competition template downloaded to {}",
            " ".if_supports_color(owo_colors::Stream::Stdout, |_| "🎉"),
            destination
                .display()
                .if_supports_color(owo_colors::Stream::Stdout, |text| text.bold())
        );
    }

    Ok(())
}
