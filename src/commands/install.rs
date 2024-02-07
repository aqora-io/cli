use crate::{
    cache::{needs_update, set_last_update_time},
    dirs::{
        init_venv, project_config_dir, project_data_dir, project_use_case_toml_path, read_pyproject,
    },
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    python::pip_install,
};
use aqora_runner::{compress::decompress, pyproject::PyProject, python::PipOptions};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use std::path::{Path, PathBuf};
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_use_case.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct GetCompetitionUseCase;

async fn download_use_case_data(dir: impl AsRef<Path>, url: Url) -> Result<()> {
    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
        error::user(
            &format!("Failed to create use case data directory: {e}"),
            "Please make sure you have permission to create directories in this directory",
        )
    })?;
    let client = reqwest::Client::new();
    let mut byte_stream = client
        .get(url)
        .send()
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to download use case data: {e}"),
                "Check your internet connection and try again",
            )
        })?
        .error_for_status()
        .map_err(|e| error::system(&format!("Failed to download use case data: {e}"), ""))?
        .bytes_stream();
    let tempfile = tempfile::NamedTempFile::new().map_err(|e| {
        error::user(
            &format!("Failed to create temporary file: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    let mut tar_file = tokio::fs::File::create(tempfile.path()).await?;
    while let Some(item) = byte_stream.next().await {
        tokio::io::copy(&mut item?.as_ref(), &mut tar_file).await?;
    }
    decompress(tempfile.path(), &dir).await.map_err(|e| {
        error::user(
            &format!("Failed to decompress use case data: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    Ok(())
}

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Install {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
    #[arg(long)]
    pub upgrade: bool,
}

pub async fn install_submission(args: Install, project: PyProject) -> Result<()> {
    let client = GraphQLClient::new(args.url.parse()?).await?;

    let m = MultiProgress::new();

    let mut use_case_pb = ProgressBar::new_spinner().with_message("Getting use case...");
    use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    use_case_pb = m.add(use_case_pb);

    let config = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| {
            error::user(
                "Project is not a submission",
                "Please make sure you are in the correct directory",
            )
        })?;

    let competition = client
        .send::<GetCompetitionUseCase>(get_competition_use_case::Variables {
            slug: config.competition.clone(),
        })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{}' not found", config.competition),
                "Please make sure the competition exists",
            )
        })?;
    let use_case_res = competition.use_case.latest.ok_or_else(|| {
        error::user(
            "No use case found",
            "Please contact the competition organizer",
        )
    })?;

    let config_dir = project_config_dir(&args.project_dir);
    tokio::fs::create_dir_all(&config_dir).await.map_err(|e| {
        error::user(
            &format!("Failed to create data directory: {e}"),
            &format!(
                "Make sure you have permissions to write to {}",
                config_dir.display()
            ),
        )
    })?;

    let use_case_toml_path = project_use_case_toml_path(&args.project_dir);
    let old_use_case = if use_case_toml_path.exists() {
        Some(PyProject::from_toml(
            tokio::fs::read_to_string(&use_case_toml_path).await?,
        )?)
    } else {
        None
    };
    let new_use_case = PyProject::from_toml(&use_case_res.pyproject_toml)?;
    let new_version = new_use_case.version().ok_or_else(|| {
        error::user(
            "Could not get project version",
            "Please make sure the project is valid",
        )
    })?;
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

    use_case_pb.finish_with_message("Use case updated");

    let mut venv_pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    venv_pb = m.add(venv_pb);

    let env = init_venv(&args.project_dir).await?;

    let should_update_use_case =
        args.upgrade || old_version.is_none() || new_version > old_version.unwrap();
    let should_update_project = should_update_use_case || needs_update(&args.project_dir).await?;

    if should_update_use_case {
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

        let download_fut = download_use_case_data(
            project_data_dir(&args.project_dir, "data"),
            use_case_data_url,
        )
        .map(move |res| {
            if res.is_ok() {
                download_pb.finish_with_message("Use case data downloaded");
            } else {
                download_pb.finish_with_message("Failed to download use case data");
            }
            res
        });

        let mut use_case_pb = ProgressBar::new_spinner().with_message("Installing use case...");
        use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        use_case_pb = m.insert_before(&venv_pb, use_case_pb);

        let cloned_pb = use_case_pb.clone();
        let options = PipOptions {
            upgrade: args.upgrade,
            ..Default::default()
        };
        let install_fut =
            pip_install(&env, [use_case_package_url], &options, &use_case_pb).map(move |res| {
                if res.is_ok() {
                    cloned_pb.finish_with_message("Use case installed");
                } else {
                    cloned_pb.finish_with_message("Failed to install use case");
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

    if should_update_project {
        let mut local_pb = ProgressBar::new_spinner().with_message("Installing local project...");
        local_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        local_pb = m.insert_before(&venv_pb, local_pb);

        pip_install(
            &env,
            [args.project_dir.to_string_lossy().to_string()],
            &PipOptions {
                upgrade: args.upgrade,
                ..Default::default()
            },
            &local_pb,
        )
        .await?;

        set_last_update_time(&args.project_dir).await?;

        local_pb.finish_with_message("Local project installed");
    }

    venv_pb.finish_with_message("Virtual environment setup");

    Ok(())
}

pub async fn install(args: Install) -> Result<()> {
    let project = read_pyproject(&args.project_dir).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;
    if aqora.is_submission() {
        install_submission(args, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run install on a submission instead",
        ))
    }
}
