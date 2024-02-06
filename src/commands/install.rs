use crate::{
    compress::decompress,
    credentials::get_access_token,
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::Id,
    pyproject::{project_data_dir, project_updated_since, PyProject},
    python::{pypi_url, PipOptions, PyEnv},
};
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

pub async fn download_use_case_data(dir: impl AsRef<Path>, url: Url) -> Result<()> {
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
    decompress(tempfile.path(), &dir).map_err(|e| {
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

    let config = project.aqora()?.as_submission()?;

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
    let competition_id = Id::parse_node_id(competition.id).map_err(|e| {
        error::system(
            &format!("Failed to parse competition ID: {e}"),
            "This is a bug, please report it",
        )
    })?;
    let use_case_res = competition.use_case.latest.ok_or_else(|| {
        error::user(
            "No use case found",
            "Please contact the competition organizer",
        )
    })?;

    let data_dir = project_data_dir(&args.project_dir);
    tokio::fs::create_dir_all(&data_dir).await.map_err(|e| {
        error::user(
            &format!("Failed to create data directory: {e}"),
            &format!(
                "Make sure you have permissions to write to {}",
                data_dir.display()
            ),
        )
    })?;

    let use_case_toml_path = data_dir.join("use_case.toml");
    let old_use_case = if use_case_toml_path.exists() {
        Some(PyProject::from_toml(
            tokio::fs::read_to_string(&use_case_toml_path).await?,
        )?)
    } else {
        None
    };
    let new_use_case = PyProject::from_toml(&use_case_res.pyproject_toml)?;
    tokio::fs::write(
        data_dir.join("use_case.toml"),
        use_case_res.pyproject_toml.as_bytes(),
    )
    .await
    .map_err(|e| {
        error::user(
            &format!("Failed to write use case pyproject.toml: {e}"),
            &format!(
                "Make sure you have permissions to write to {}",
                data_dir.display()
            ),
        )
    })?;

    let last_update_path = data_dir.join("last-update");
    let last_update = if last_update_path.exists() {
        Some(
            chrono::DateTime::parse_from_rfc3339(
                &tokio::fs::read_to_string(data_dir.join("last-update")).await?,
            )
            .map_err(|e| {
                error::system(
                    &format!("Failed to read last update time: {e}"),
                    "Try running `aqora install` again",
                )
            })?,
        )
    } else {
        None
    };

    use_case_pb.finish_with_message("Use case updated");

    let mut venv_pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    venv_pb = m.add(venv_pb);

    let env = PyEnv::init(&args.project_dir).await?;

    let should_update_use_case = args.upgrade
        || old_use_case.is_none()
        || new_use_case.version()? > old_use_case.as_ref().unwrap().version()?;
    let should_update_project = should_update_use_case
        || last_update.is_none()
        || project_updated_since(&args.project_dir, last_update.unwrap());

    if should_update_use_case {
        let mut download_pb =
            ProgressBar::new_spinner().with_message("Downloading use case data...");
        download_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        download_pb = m.insert_before(&venv_pb, download_pb);

        let download_fut = download_use_case_data(
            data_dir.join("data"),
            use_case_res
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
                .clone(),
        )
        .map(move |res| {
            if res.is_ok() {
                download_pb.finish_with_message("Use case data downloaded");
            } else {
                download_pb.finish_with_message("Failed to download use case data");
            }
            res
        });

        let use_case_package = format!(
            "use-case-{}=={}",
            competition_id.to_package_id(),
            use_case_res.version
        );
        let extra_index_urls = {
            let url = args.url.clone().parse()?;
            vec![pypi_url(&url, get_access_token(url.clone()).await?)?]
        };

        let mut use_case_pb = ProgressBar::new_spinner().with_message("Installing use case...");
        use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        use_case_pb = m.insert_before(&venv_pb, use_case_pb);

        let cloned_pb = use_case_pb.clone();
        let options = PipOptions {
            upgrade: args.upgrade,
            extra_index_urls,
            ..Default::default()
        };
        let install_fut = env
            .pip_install([use_case_package], &options, Some(&use_case_pb))
            .map(move |res| {
                if res.is_ok() {
                    cloned_pb.finish_with_message("Use case installed");
                } else {
                    cloned_pb.finish_with_message("Failed to install use case");
                }
                res
            });

        futures::future::try_join(download_fut, install_fut).await?;
    }

    if should_update_project {
        let mut local_pb = ProgressBar::new_spinner().with_message("Installing local project...");
        local_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        local_pb = m.insert_before(&venv_pb, local_pb);

        env.pip_install(
            [args.project_dir.to_string_lossy().to_string()],
            &PipOptions {
                upgrade: args.upgrade,
                ..Default::default()
            },
            Some(&local_pb),
        )
        .await?;

        tokio::fs::write(last_update_path, chrono::Utc::now().to_rfc3339().as_bytes())
            .await
            .map_err(|e| {
                error::user(
                    &format!("Failed to write last-update: {e}"),
                    &format!(
                        "Make sure you have permissions to write to {}",
                        data_dir.join("last-update").display()
                    ),
                )
            })?;

        local_pb.finish_with_message("Local project installed");
    }

    venv_pb.finish_with_message("Virtual environment setup");

    Ok(())
}

pub async fn install(args: Install) -> Result<()> {
    let project = PyProject::for_project(&args.project_dir)?;
    let aqora = project.aqora()?;
    if aqora.is_submission() {
        install_submission(args, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run install on a submission instead",
        ))
    }
}
