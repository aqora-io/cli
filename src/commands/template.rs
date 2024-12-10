use crate::{
    colors::ColorChoiceExt,
    commands::{
        install::{install, Install},
        login::check_login,
        GlobalArgs,
    },
    dirs::pyproject_path,
    download::download_archive,
    error::{self, Result},
    git::init_repository,
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

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_viewer_enabled_entities.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug,Clone"
)]
pub struct GetViewerEnabledEntities;

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
    let logged_in = check_login(global.clone(), &m).await?;

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

    let mut pb = ProgressBar::new_spinner().with_message("Fetching competition...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb = m.add(pb);

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

    let organization = if logged_in {
        let viewer = client
            .send::<GetViewerEnabledEntities>(get_viewer_enabled_entities::Variables {
                resource: competition.id,
                action: get_viewer_enabled_entities::Action::CREATE_SUBMISSION_VERSION,
            })
            .await?
            .viewer;
        let viewer_orgs = viewer
            .entities
            .nodes
            .iter()
            .filter(|entity| entity.id != viewer.id)
            .cloned()
            .collect::<Vec<_>>();
        if !viewer_orgs.is_empty() {
            m.suspend(|| -> Result<_> {
                let mut items = vec![format!("@{} (Myself)", viewer.username)];
                items.extend(viewer_orgs.iter().map(|org| {
                    format!("@{} ({})", org.username.clone(), org.display_name.clone())
                }));
                Result::Ok(
                    dialoguer::FuzzySelect::with_theme(global.color.dialoguer().as_ref())
                        .with_prompt("Would you like to submit with a team? (Press ESC to skip)")
                        .items(&items)
                        .interact_opt()
                        .map_err(|err| {
                            error::system(
                                &format!("Could not select organization: {err}"),
                                "Please try again",
                            )
                        })?
                        .and_then(|index| {
                            if index == 0 {
                                None
                            } else {
                                viewer_orgs.into_iter().nth(index - 1)
                            }
                        }),
                )
            })?
        } else {
            None
        }
    } else {
        None
    };

    pb.set_message("Downloading competition template...");
    match download_archive(download_url, &destination, &pb).await {
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

    if let Some(organization) = organization {
        let toml_path = pyproject_path(&destination);
        let mut doc = tokio::fs::read_to_string(&toml_path)
            .await
            .map_err(|err| {
                error::system(
                    &format!("Failed to read {}: {err}", toml_path.display()),
                    "Contact the competition organizer",
                )
            })?
            .parse::<toml_edit::DocumentMut>()
            .map_err(|err| {
                error::system(
                    &format!("Failed to parse {}: {err}", toml_path.display()),
                    "Contact the competition organizer",
                )
            })?;
        let aqora_config = doc
            .get_mut("tool")
            .and_then(|tool| tool.as_table_mut())
            .and_then(|tool| tool.get_mut("aqora"))
            .and_then(|aqora| aqora.as_table_mut())
            .ok_or_else(|| {
                error::system(
                    &format!(
                        "Failed to parse {}: Could not find tool.aqora",
                        toml_path.display()
                    ),
                    "Contact the competition organizer",
                )
            })?;
        aqora_config["entity"] = toml_edit::value(organization.username.clone());
        tokio::fs::write(&toml_path, doc.to_string())
            .await
            .map_err(|err| {
                error::system(
                    &format!("Failed to write {}: {err}", toml_path.display()),
                    "Check the permissions of the file",
                )
            })?;
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
