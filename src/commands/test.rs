use crate::{
    cache::{needs_update, set_last_update_time},
    error::{self, Result},
    pipeline::{Pipeline, PipelineConfig},
    pyproject::{project_data_dir, PyProject},
    python::{PipOptions, PyEnv},
};
use clap::Args;
use indicatif::{MultiProgress, ProgressBar};
use std::path::PathBuf;

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

pub async fn test_submission(args: Test, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let data_dir = project_data_dir(&args.project_dir);
    let use_case_toml_path = data_dir.join("use_case.toml");
    let data_path = data_dir.join("data").join("data");
    if !data_dir.exists() || !use_case_toml_path.exists() || !data_path.exists() {
        return Err(error::user(
            "Project not setup",
            "Run `aqora install` first.",
        ));
    }
    let use_case_toml = PyProject::from_toml(tokio::fs::read_to_string(use_case_toml_path).await?)
        .map_err(|e| {
            error::system(
                &format!("Failed to read use case: {e}"),
                "Try running `aqora install` again",
            )
        })?;
    let use_case = use_case_toml.aqora()?.as_use_case()?;

    let submission = project.aqora()?.as_submission()?;

    let mut venv_pb = ProgressBar::new_spinner().with_message("Applying changes");
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    venv_pb = m.add(venv_pb);

    let env = PyEnv::init(&args.project_dir).await?;

    if needs_update(&args.project_dir).await? {
        env.pip_install(
            [args.project_dir.to_string_lossy().to_string()],
            &PipOptions {
                no_deps: true,
                ..Default::default()
            },
            Some(&venv_pb),
        )
        .await?;
        set_last_update_time(&args.project_dir).await?;
        venv_pb.finish_with_message("Changes applied");
    } else {
        venv_pb.finish_with_message("Already up to date");
    }

    let mut pipeline_pb = ProgressBar::new_spinner().with_message("Running pipeline...");
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
