use super::{install, Install};
use crate::{
    compress::decompress,
    credentials::get_access_token,
    error::{self, Error, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::Id,
    pipeline::{Pipeline, PipelineConfig},
    pyproject::{project_data_dir, PackageName, PyProject},
    python::{async_generator, pypi_url, AsyncIterator, PipOptions, PyEnv},
};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use pyo3::{prelude::*, types::PyModule, Python};
use std::path::{Path, PathBuf};
use tempfile::tempfile;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_use_case.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct GetCompetitionUseCase;

async fn get_use_case(
    client: &GraphQLClient,
    competition_id: Id,
) -> Result<get_competition_use_case::GetCompetitionUseCaseNodeOnCompetitionUseCase> {
    let use_case = match client
        .send::<GetCompetitionUseCase>(get_competition_use_case::Variables {
            id: competition_id.to_node_id(),
        })
        .await?
        .node
    {
        get_competition_use_case::GetCompetitionUseCaseNode::Competition(c) => {
            if let Some(use_case) = c.use_case {
                use_case
            } else {
                return Err(error::user(
                    "No use case found",
                    "Please contact the competition organizer",
                ));
            }
        }
        _ => {
            return Err(error::user(
                "No use case found",
                "Please contact the competition organizer",
            ));
        }
    };
    Ok(use_case)
}

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

impl From<Test> for Install {
    fn from(args: Test) -> Self {
        Install {
            url: args.url,
            project_dir: args.project_dir,
            upgrade: false,
        }
    }
}

pub async fn download_use_case_data(project_dir: impl AsRef<Path>, url: Url) -> Result<PathBuf> {
    let dir = project_data_dir(project_dir.as_ref()).join("data");
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
    Ok(dir.join("data"))
}

pub async fn test_submission(args: Test, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let submission = project.aqora()?.as_submission()?;

    let mut venv_pb = ProgressBar::new_spinner().with_message("Applying changes");
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    venv_pb = m.add(venv_pb);

    let env = PyEnv::init(&args.project_dir).await?;

    env.pip_install(
        [args.project_dir.to_string_lossy().to_string()],
        &PipOptions {
            no_deps: true,
            ..Default::default()
        },
        Some(&venv_pb),
    )
    .await?;

    venv_pb.finish_with_message("Changes applied");

    let mut download_pb = ProgressBar::new_spinner().with_message("Downloading use case data...");
    download_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    download_pb = m.add(download_pb);

    let client = GraphQLClient::new(args.url.parse()?).await?;
    let use_case_res = get_use_case(&client, submission.competition).await?;
    let use_case_pyproject = PyProject::from_toml(use_case_res.pyproject_toml)?;
    let use_case = use_case_pyproject.aqora()?.as_use_case()?;

    let data_path = download_use_case_data(
        &args.project_dir,
        use_case_res
            .files
            .iter()
            .find(|file| matches!(file.kind, get_competition_use_case::UseCaseFileKind::DATA))
            .ok_or_else(|| {
                error::system(
                    "No use case data found",
                    "Please contact the competition organizer",
                )
            })?
            .download_url
            .clone(),
    )
    .await?;

    download_pb.finish_with_message("Use case data downloaded");

    let mut pipeline_pb = ProgressBar::new_spinner().with_message("Downloading use case data...");
    pipeline_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pipeline_pb = m.add(pipeline_pb);

    let config = PipelineConfig {
        data: data_path.canonicalize()?,
    };
    let pipeline = Pipeline::import(use_case, submission, config)?;
    let score = pipeline
        .aggregate(pipeline.evaluate(pipeline.generator()?, pipeline.evaluator()))
        .await?;

    pipeline_pb.finish_with_message(format!("Done: {score}"));

    Ok(())
}

pub async fn test(args: Test) -> Result<()> {
    if !project_data_dir(&args.project_dir).exists() {
        install(args.clone().into()).await?;
    }
    let project = PyProject::for_project(&args.project_dir)?;
    let aqora = project.aqora()?;
    if aqora.is_submission() {
        test_submission(args, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run test on a submission instead",
        ))
    }
}
