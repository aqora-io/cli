use crate::{
    commands::{
        clean::{clean, Clean},
        install::{install, Install},
        GlobalArgs,
    },
    download::download_archive,
    error::{self, Result},
    git::init_repository,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use indicatif::MultiProgress;
use owo_colors::OwoColorize;
use serde::Serialize;
use std::path::PathBuf;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_template.graphql",
    schema_path = "schema.graphql",
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
    let m = MultiProgress::new();

    let client = global.graphql_client().await?;

    let destination = args
        .destination
        .unwrap_or_else(|| PathBuf::from(args.competition.clone()));

    let pb = m.add(global.spinner().with_message("Fetching competition..."));

    if destination.exists() {
        if let Ok(mut read_dir) = tokio::fs::read_dir(&destination).await {
            if read_dir.next_entry().await?.is_some() {
                let unpack = pb.suspend(|| {
                    global
                        .confirm()
                        .with_prompt(format!(
                            "The destination '{}' already exists and is not empty.
This may overwrite files. Do you want to continue?",
                            destination.display()
                        ))
                        .default(true)
                        .interact()
                })?;
                if !unpack {
                    pb.finish_with_message(
                        "Aborted. Please choose a different destination directory",
                    );
                    return Ok(());
                }
            }
        } else {
            return Err(error::user(
                &format!(
                    "Destination directory '{}' could not be read",
                    destination.display()
                ),
                "Please check the permissions or specify a different destination",
            ));
        }
    }

    let competition = client
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
        })?;

    let use_case = competition.use_case.latest.ok_or_else(|| {
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
    match download_archive(&client, download_url, &destination, &pb).await {
        Ok(_) => {
            init_repository(&pb, &destination, None)
                .inspect_err(|e| {
                    tracing::warn!(
                        "Failed to create a Git repository: {}. Skipping git init.",
                        e
                    )
                })
                .ok();
            pb.finish_with_message(format!(
                "Competition template downloaded to {}",
                destination.display()
            ))
        }
        Err(error) => {
            pb.finish_with_message("Failed to download competition template");
            return Err(error);
        }
    }

    if args.no_install {
        let clean_global = GlobalArgs {
            project: destination.clone(),
            ..global
        };
        clean(Clean, clean_global).await?;
    } else {
        let install_global = GlobalArgs {
            project: destination.clone(),
            ..global
        };
        install(
            Install {
                competition: Some(args.competition),
                upgrade: true,
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
