use crate::{
    dirs::{
        init_venv, project_data_dir, project_last_run_dir, project_use_case_toml_path,
        read_pyproject,
    },
    error::{self, Result},
};
use aqora_config::PyProject;
use aqora_runner::{
    pipeline::{
        EvaluationError, EvaluationResult, Evaluator, Pipeline, PipelineConfig, PipelineImportError,
    },
    python::serde_pickle_opt,
};
use clap::Args;
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar};
use owo_colors::{OwoColorize, Stream as OwoStream, Style};
use pyo3::prelude::*;
use pyo3::{exceptions::PyException, Python};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = ".")]
    pub project: PathBuf,
    #[arg(long)]
    pub uv: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
struct LastRunItem {
    #[serde(with = "serde_pickle_opt")]
    input: Option<PyObject>,
    results: EvaluationResult,
    error: Option<EvaluationError>,
}

fn evaluate(
    evaluator: Evaluator,
    inputs: impl Stream<Item = PyResult<PyObject>>,
    last_run_dir: impl AsRef<Path>,
    pb: ProgressBar,
) -> impl Stream<Item = Result<EvaluationResult, (EvaluationResult, EvaluationError)>> {
    let evaluator = Arc::new(evaluator);
    inputs
        .map_ok(move |input| (input, evaluator.clone()))
        .enumerate()
        .then(|(index, result)| async move {
            match result {
                Ok((input, evaluator)) => match evaluator.evaluate(input.clone()).await {
                    Ok(results) => (
                        index,
                        LastRunItem {
                            input: Some(input),
                            results,
                            error: None,
                        },
                    ),
                    Err((results, error)) => (
                        index,
                        LastRunItem {
                            input: Some(input),
                            results,
                            error: Some(error),
                        },
                    ),
                },
                Err(err) => (
                    index,
                    LastRunItem {
                        input: None,
                        results: EvaluationResult::new(),
                        error: Some(EvaluationError::Python(err)),
                    },
                ),
            }
        })
        .map(move |(index, item)| (index, item, last_run_dir.as_ref().to_path_buf(), pb.clone()))
        .then(|(index, item, last_run_dir, pb)| async move {
            let filename = last_run_dir.join(format!("{:016}.msgpack", index));
            let err = match std::fs::File::create(&filename) {
                Ok(mut file) => {
                    if let Err(err) = rmp_serde::encode::write(&mut file, &item) {
                        Some(err.to_string())
                    } else {
                        None
                    }
                }
                Err(err) => Some(err.to_string()),
            };
            if let Some(err) = err {
                pb.println(format!(
                    "{}: Failed to write to file {}: {err}",
                    filename.display(),
                    format!("[{index} ERR]")
                        .if_supports_color(OwoStream::Stdout, |text| text.red()),
                    err = err
                ));
                return Err((
                    item.results,
                    EvaluationError::Python(PyException::new_err(err)),
                ));
            }

            let is_ok = item.error.is_none();
            let message = if is_ok {
                "Success"
            } else if item.input.is_some() {
                "Evaluation error"
            } else {
                "Input generation error"
            };
            pb.println(format!(
                "{}: {}",
                format!("[{index} {}]", if is_ok { "OK" } else { "FAIL" }).if_supports_color(
                    OwoStream::Stdout,
                    |text| {
                        text.style(if is_ok {
                            Style::new().green()
                        } else {
                            Style::new().red()
                        })
                    }
                ),
                message
            ));

            if let Some(error) = item.error {
                Err((item.results, error))
            } else {
                Ok(item.results)
            }
        })
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

    let last_run_dir = project_last_run_dir(&args.project);
    tokio::fs::create_dir_all(&last_run_dir)
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to create last run directory: {e}"),
                &format!(
                    "Make sure you have permissions to write to {}",
                    last_run_dir.display()
                ),
            )
        })?;
    let run_success_file = last_run_dir.join("success");
    if run_success_file.exists() {
        tokio::fs::remove_file(&run_success_file)
            .await
            .map_err(|e| {
                error::user(
                    &format!("Failed to write to {}: {}", last_run_dir.display(), e),
                    &format!(
                        "Make sure you have permissions to write to {}",
                        last_run_dir.display()
                    ),
                )
            })?;
    }

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
        .aggregate(evaluate(
            pipeline.evaluator(),
            generator,
            last_run_dir.clone(),
            pipeline_pb.clone(),
        ))
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

    tokio::fs::File::create(run_success_file)
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to write to {}: {}", last_run_dir.display(), e),
                &format!(
                    "Make sure you have permissions to write to {}",
                    last_run_dir.display()
                ),
            )
        })?;
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
