use crate::{
    dirs::{init_venv, project_data_dir, project_use_case_toml_path, read_pyproject},
    error::{self, Result},
};
use aqora_config::PyProject;
use aqora_runner::pipeline::{EvaluationError, Pipeline, PipelineConfig, PipelineImportError};
use clap::Args;
use indicatif::{MultiProgress, ProgressBar};
use pyo3::Python;
use std::path::PathBuf;

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = ".")]
    pub project: PathBuf,
    #[arg(long)]
    pub uv: Option<PathBuf>,
}

pub async fn test_submission(args: Test, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let submission = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| error::user("Submission config is not valid", ""))?;

    let use_case_toml_path = project_use_case_toml_path(&args.project);
    let data_path = project_data_dir(&args.project, "data");
    if !use_case_toml_path.exists() || !data_path.exists() {
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
    let use_case = use_case_toml
        .aqora()
        .and_then(|aqora| aqora.as_use_case())
        .ok_or_else(|| {
            error::system(
                "Use case config is not valid",
                "Check with your competition provider",
            )
        })?;

    let mut pipeline_pb = ProgressBar::new_spinner().with_message("Running pipeline...");
    pipeline_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pipeline_pb = m.add(pipeline_pb);

    let env = init_venv(&args.project, args.uv.as_ref(), &pipeline_pb).await?;

    let config = PipelineConfig {
        data: data_path.canonicalize()?,
    };
    let pipeline = match Pipeline::import(&env, use_case, submission, config) {
        Ok(pipeline) => pipeline,
        Err(PipelineImportError::Python(e)) => {
            pipeline_pb.finish_with_message("Failed to import pipeline");
            Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
            return Err(error::user(
                "Failed to import pipeline",
                "Check the above error and try again",
            ));
        }
        Err(e) => {
            pipeline_pb.finish_with_message("Failed to import pipeline");
            return Err(error::system(
                &format!("Failed to import pipeline: {e}"),
                "Check the pipeline configuration and try again",
            ));
        }
    };
    let generator = match pipeline.generator() {
        Ok(g) => g,
        Err(e) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
            return Err(error::user(
                "Unable to generate an inputs",
                "Check the above error and try again",
            ));
        }
    };
    let score = match pipeline
        .aggregate(pipeline.evaluate(generator, pipeline.evaluator()))
        .await
    {
        Ok(Some(score)) => score,
        Ok(None) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            return Err(error::system(
                "No score returned. Use case may not have any inputs",
                "",
            ));
        }
        Err(EvaluationError::Python(e)) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
            return Err(error::user(
                "Failed to run pipeline",
                "Check the above error and try again",
            ));
        }
        Err(e) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            return Err(error::user(
                &format!("Failed to run pipeline: {e}"),
                "Check the pipeline configuration and try again",
            ));
        }
    };

    pipeline_pb.finish_with_message(format!("Done: {score}"));

    Ok(())
}

pub async fn test(args: Test) -> Result<()> {
    let project = read_pyproject(&args.project).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;
    if aqora.is_submission() {
        test_submission(args, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run test on a submission instead",
        ))
    }
}
