use crate::{
    commands::GlobalArgs,
    dirs::{project_config_dir, project_data_dir, project_use_case_toml_path, read_pyproject},
    download::download_tar_gz,
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    python::pip_install,
};
use aqora_config::PyProject;
use aqora_runner::python::{PipOptions, PipPackage};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use serde::Serialize;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_use_case.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct GetCompetitionUseCase;

#[derive(Args, Debug, Default, Serialize)]
pub struct Install {
    #[arg(long, short)]
    pub upgrade: bool,
    pub competition: Option<String>,
}

pub async fn install_submission(
    args: Install,
    global: GlobalArgs,
    project: PyProject,
) -> Result<()> {
    let client = GraphQLClient::new(global.url.parse()?).await?;

    let m = MultiProgress::new();

    let mut venv_pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    venv_pb = m.add(venv_pb);

    let mut use_case_pb = ProgressBar::new_spinner().with_message("Getting use case...");
    use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    use_case_pb = m.insert_before(&venv_pb, use_case_pb);

    let config = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| {
            error::user(
                "Project is not a submission",
                "Please make sure you are in the correct directory",
            )
        })?;

    let slug = args
        .competition
        .as_ref()
        .or(config.competition.as_ref())
        .ok_or_else(|| {
            error::user(
                "No competition provided",
                "Please specify a competition in either the pyproject.toml or the command line",
            )
        })?;
    let competition = client
        .send::<GetCompetitionUseCase>(get_competition_use_case::Variables { slug: slug.clone() })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{slug}' not found"),
                "Please make sure the competition exists",
            )
        })?;

    let config_dir = project_config_dir(&global.project);
    tokio::fs::create_dir_all(&config_dir).await.map_err(|e| {
        error::user(
            &format!("Failed to create data directory: {e}"),
            &format!(
                "Make sure you have permissions to write to {}",
                config_dir.display()
            ),
        )
    })?;

    let use_case_toml_path = project_use_case_toml_path(&global.project);
    let old_use_case = if use_case_toml_path.exists() {
        Some(PyProject::from_toml(
            tokio::fs::read_to_string(&use_case_toml_path).await?,
        )?)
    } else {
        None
    };
    let old_version = old_use_case
        .as_ref()
        .map(|use_case| {
            use_case.version().ok_or_else(|| {
                error::user(
                    "Could not get project version",
                    "Please make sure the project is valid",
                )
            })
        })
        .transpose()?;

    let env = global.init_venv(&venv_pb).await?;

    let use_case_package_name = competition.use_case.name.clone();
    let use_case_res = competition.use_case.latest.ok_or_else(|| {
        error::user(
            "No use case found",
            "Please contact the competition organizer",
        )
    })?;
    let new_use_case = PyProject::from_toml(&use_case_res.pyproject_toml)?;
    let new_version = new_use_case.version().ok_or_else(|| {
        error::user(
            "Could not get project version",
            "Please make sure the project is valid",
        )
    })?;

    use_case_pb.finish_with_message("Use case updated");

    let should_update = args.upgrade || old_version.is_none() || new_version > old_version.unwrap();

    if should_update {
        let mut download_pb =
            ProgressBar::new_spinner().with_message("Downloading use case data...");
        download_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        download_pb = m.insert_before(&venv_pb, download_pb);

        let use_case_data_url = use_case_res
            .files
            .iter()
            .find(|file| {
                matches!(
                    file.kind,
                    get_competition_use_case::ProjectVersionFileKind::DATA
                )
            })
            .ok_or_else(|| {
                error::system(
                    "No use case data found",
                    "Please contact the competition organizer",
                )
            })?
            .download_url
            .clone();

        let use_case_package_url = use_case_res
            .files
            .iter()
            .find(|file| {
                matches!(
                    file.kind,
                    get_competition_use_case::ProjectVersionFileKind::PACKAGE
                )
            })
            .ok_or_else(|| {
                error::system(
                    "No use case data found",
                    "Please contact the competition organizer",
                )
            })?
            .download_url
            .clone();

        download_pb.set_message("Downloading use case data");
        let download_fut = download_tar_gz(
            use_case_data_url,
            project_data_dir(&global.project, "data"),
            &download_pb,
        )
        .inspect(|res| {
            if res.is_ok() {
                download_pb.finish_with_message("Use case data downloaded")
            } else {
                download_pb.finish_with_message("Failed to download use case data")
            }
        });

        let mut use_case_pb = ProgressBar::new_spinner().with_message("Installing packages...");
        use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        use_case_pb = m.insert_before(&venv_pb, use_case_pb);

        let cloned_pb = use_case_pb.clone();
        let options = PipOptions {
            upgrade: args.upgrade,
            ..global.pip_options()
        };
        let install_fut = pip_install(
            &env,
            [
                PipPackage::tar(use_case_package_name, use_case_package_url.to_string()),
                PipPackage::editable(&global.project),
            ],
            &options,
            &use_case_pb,
        )
        .map(move |res| {
            if res.is_ok() {
                cloned_pb.finish_with_message("Packages installed");
            } else {
                cloned_pb.finish_with_message("Failed to install packages");
            }
            res
        });

        futures::future::try_join(download_fut, install_fut).await?;

        tokio::fs::write(use_case_toml_path, use_case_res.pyproject_toml.as_bytes())
            .await
            .map_err(|e| {
                error::user(
                    &format!("Failed to write use case pyproject.toml: {e}"),
                    &format!(
                        "Make sure you have permissions to write to {}",
                        config_dir.display()
                    ),
                )
            })?;
    }

    venv_pb.finish_with_message("Virtual environment setup");

    Ok(())
}

pub async fn install_use_case(args: Install, global: GlobalArgs) -> Result<()> {
    let m = MultiProgress::new();

    let mut pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb = m.add(pb);

    let env = global.init_venv(&pb).await?;

    pip_install(
        &env,
        [PipPackage::editable(&global.project)],
        &PipOptions {
            upgrade: args.upgrade,
            ..global.pip_options()
        },
        &pb,
    )
    .await?;

    pb.finish_with_message("Virtual environment setup");

    Ok(())
}

pub async fn install(args: Install, global: GlobalArgs) -> Result<()> {
    let project = read_pyproject(&global.project).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;
    if aqora.is_submission() {
        install_submission(args, global, project).await
    } else {
        install_use_case(args, global).await
    }
}
