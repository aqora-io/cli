use crate::{
    credentials::get_access_token,
    error::{self, Error, Result},
    id::Id,
    pyproject::{PackageName, PyProject},
    python::{pypi_url, PipOptions, PyEnv},
};
use clap::Args;
use futures::TryFutureExt;
use indicatif::{MultiProgress, ProgressBar};
use pyo3::types::PyModule;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
    #[arg(long)]
    pub upgrade: bool,
}

pub async fn run_pipeline(args: Test, project: PyProject) -> Result<f32> {
    // TODO fix me
    let use_case_project = PyProject::for_project("examples/use_case")?;
    let out = pyo3::Python::with_gil(|py| {
        let pipeline = PyModule::from_code(
            py,
            include_str!("../py/pipeline.py"),
            "pipeline.py",
            "pipeline",
        )?;
        let layer_cls = pipeline.getattr("Layer")?;
        let pipeline_cls = pipeline.getattr("Pipeline")?;

        let aqora = use_case_project.aqora();
        let generator = aqora
            .generator
            .ok_or_else(|| {
                error::user(
                    "No generator given",
                    "Make sure the generator is set in the aqora section \
                    of your pyproject.toml",
                )
            })?
            .path
            .import(py)?
            .call0()?;
        let aggregator = aqora
            .aggregator
            .ok_or_else(|| {
                error::user(
                    "No aggregator given",
                    "Make sure the aggregator is set in the aqora section \
                    of your pyproject.toml",
                )
            })?
            .path
            .import(py)?;

        let layers = aqora
            .layers
            .into_iter()
            .map(|layer| {
                let evaluator = layer.evaluate.path.import(py)?;
                let metric = if let Some(metric) = layer.metric {
                    Some(metric.path.import(py)?)
                } else {
                    None
                };
                layer_cls.call1((evaluator, metric))
            })
            .collect::<pyo3::PyResult<Vec<_>>>()?;

        let pipeline = pipeline_cls.call1((generator, layers, aggregator))?;

        Ok::<_, Error>(pyo3_asyncio::into_future_with_locals(
            &pyo3_asyncio::tokio::get_current_locals(py)?,
            pipeline.call_method0("run")?,
        )?)
    })?
    .await?;
    Ok(pyo3::Python::with_gil(|py| out.extract(py))?)
}

pub async fn test_submission(
    args: Test,
    competition_id: Id,
    user_id: Id,
    project: PyProject,
) -> Result<()> {
    let m = MultiProgress::new();

    let mut pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb = m.add(pb);

    pb.set_message("Setting up virtual environment");
    let env = PyEnv::init(&args.project_dir).await?;

    let use_case_package = format!("use-case-{}", competition_id.to_package_id());
    let url = args.url.parse()?;
    env.pip_install(
        [
            use_case_package,
            args.project_dir.to_string_lossy().to_string(),
        ],
        &PipOptions {
            upgrade: args.upgrade,
            extra_index_urls: vec![pypi_url(&url, get_access_token(url.clone()).await?)?],
        },
        Some(&pb),
    )
    .await?;

    pb.finish_with_message("Virtual environment setup");

    let mut pb = ProgressBar::new_spinner().with_message("Running tests");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb = m.add(pb);

    let score = run_pipeline(args, project).await?;

    pb.finish_with_message("Tests complete");

    println!("Score: {}", score);

    Ok(())
}

pub async fn test(args: Test) -> Result<()> {
    let project = PyProject::for_project(&args.project_dir)?;

    match project.name()? {
        PackageName::UseCase { .. } => Err(error::user(
            "Use cases not supported",
            "Run test on a submission instead",
        )),
        PackageName::Submission { competition, user } => {
            test_submission(args, competition, user, project).await
        }
    }
}
