use crate::{
    commands::GlobalArgs,
    dirs::{
        init_venv, project_data_dir, project_last_run_dir, project_use_case_toml_path,
        read_pyproject,
    },
    error::{self, Result},
};
use aqora_config::{PyProject, Version};
use aqora_runner::pipeline::{
    EvaluateAllInfo, EvaluateInputInfo, EvaluationError, EvaluationResult, Evaluator, Pipeline,
    PipelineConfig,
};
use clap::Args;
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar};
use owo_colors::{OwoColorize, Stream as OwoStream, Style};
use pyo3::prelude::*;
use pyo3::{exceptions::PyException, Python};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    pin::Pin,
    sync::{atomic::AtomicU32, Arc},
};

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long)]
    pub test: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct LastRunResult {
    #[serde(flatten)]
    pub info: EvaluateAllInfo,
    pub use_case_version: Option<Version>,
    pub submission_version: Option<Version>,
}

fn evaluate(
    evaluator: Evaluator,
    inputs: impl Stream<Item = (usize, PyResult<PyObject>)>,
    last_run_dir: impl AsRef<Path>,
    pb: ProgressBar,
) -> impl Stream<Item = Result<EvaluationResult, (EvaluationResult, EvaluationError)>> {
    let evaluator = Arc::new(evaluator);
    inputs
        .map(move |input| (input, evaluator.clone()))
        .then(|((index, result), evaluator)| async move {
            match result {
                Ok(input) => match evaluator.evaluate(input.clone()).await {
                    Ok(result) => (
                        index,
                        EvaluateInputInfo {
                            input: Some(input),
                            result,
                            error: None,
                        },
                    ),
                    Err((result, error)) => (
                        index,
                        EvaluateInputInfo {
                            input: Some(input),
                            result,
                            error: Some(error),
                        },
                    ),
                },
                Err(err) => (
                    index,
                    EvaluateInputInfo {
                        input: None,
                        result: EvaluationResult::new(),
                        error: Some(EvaluationError::Python(err)),
                    },
                ),
            }
        })
        .map(move |(index, item)| (index, item, last_run_dir.as_ref().to_path_buf(), pb.clone()))
        .then(|(index, item, last_run_dir, pb)| async move {
            let filename = last_run_dir.join(format!("{index}.msgpack"));
            let index = index + 1;
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
                    item.result,
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
                "{} {}",
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
                Err((item.result, error))
            } else {
                Ok(item.result)
            }
        })
}

fn last_run_items(
    last_run_dir: impl AsRef<Path>,
    tests: Vec<usize>,
) -> impl Stream<Item = Result<(usize, EvaluateInputInfo), (usize, std::io::Error)>> {
    futures::stream::iter(tests).map(move |index| {
        if index == 0 {
            return Err((
                index,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Could not load test {index}: Test index starts from 1"),
                ),
            ));
        }
        match std::fs::File::open(last_run_dir.as_ref().join(format!("{}.msgpack", index - 1))) {
            Ok(file) => match rmp_serde::from_read(file) {
                Ok(item) => Ok((index - 1, item)),
                Err(err) => Err((
                    index,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Could not load test {index}: {err}"),
                    ),
                )),
            },
            Err(err) => Err((index, err)),
        }
    })
}

pub async fn test_submission(args: Test, global: GlobalArgs, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let submission = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| error::user("Submission config is not valid", ""))?;

    let use_case_toml_path = project_use_case_toml_path(&global.project);
    let data_path = project_data_dir(&global.project, "data");
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

    let last_run_dir = project_last_run_dir(&global.project);
    let last_run_result_file = last_run_dir.join("result.msgpack");
    if args.test.is_empty() {
        if last_run_dir.exists() {
            tokio::fs::remove_dir_all(&last_run_dir)
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
    } else if last_run_result_file.exists() {
        tokio::fs::remove_file(&last_run_result_file)
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
    tokio::fs::create_dir_all(&last_run_dir)
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

    let modified_use_case = {
        let mut use_case = use_case.clone();
        if let Err(err) = use_case.replace_refs(&submission.refs) {
            return Err(error::system(
                &format!("Failed to import pipeline: {err}"),
                "Check the pipeline configuration and try again",
            ));
        }
        use_case
    };
    let config = PipelineConfig {
        data: data_path.canonicalize()?,
    };

    let mut pipeline_pb = ProgressBar::new_spinner().with_message("Running pipeline...");
    pipeline_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pipeline_pb = m.add(pipeline_pb);

    let env = init_venv(
        &global.project,
        global.uv.as_ref(),
        &pipeline_pb,
        global.color,
    )
    .await?;

    let pipeline = match Pipeline::import(&env, &modified_use_case, config) {
        Ok(pipeline) => pipeline,
        Err(err) => {
            pipeline_pb.finish_with_message("Failed to import pipeline");
            Python::with_gil(|py| err.print_and_set_sys_last_vars(py));
            return Err(error::user(
                "Failed to import pipeline",
                "Check the above error and try again",
            ));
        }
    };

    let (num_inputs, generator) = if args.test.is_empty() {
        match pipeline.generator() {
            Ok(generator) => {
                let num_inputs = Arc::new(AtomicU32::new(0));
                let inputs_cloned = num_inputs.clone();
                (
                    num_inputs,
                    Box::pin(
                        generator
                            .inspect(move |_| {
                                inputs_cloned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            })
                            .enumerate(),
                    )
                        as Pin<Box<dyn Stream<Item = (usize, PyResult<PyObject>)> + Send + Sync>>,
                )
            }
            Err(error) => {
                pipeline_pb.finish_with_message("Failed to run pipeline");
                Python::with_gil(|py| error.print_and_set_sys_last_vars(py));
                return Err(error::user(
                    "Unable to generate an inputs",
                    "Check the above error and try again",
                ));
            }
        }
    } else {
        let inputs = last_run_items(&last_run_dir, args.test.clone())
            .map_ok(move |(index, item)| {
                if let Some(input) = item.input {
                    (index, Ok(input))
                } else if let Some(EvaluationError::Python(error)) = item.error {
                    (index, Err(error))
                } else {
                    (index, Err(PyException::new_err("No input or error")))
                }
            })
            .map_err(|(index, err)| {
                error::user(
                    &format!("Failed to read last run data for {index}: {err}"),
                    "Check the above error and try again",
                )
            })
            .try_collect::<Vec<_>>()
            .await?;
        (
            Arc::new(AtomicU32::new(inputs.len() as u32)),
            Box::pin(futures::stream::iter(inputs)) as _,
        )
    };

    let result = match pipeline
        .aggregate(evaluate(
            pipeline.evaluator(),
            generator,
            last_run_dir.clone(),
            pipeline_pb.clone(),
        ))
        .await
    {
        Ok(Some(score)) => {
            pipeline_pb.finish_with_message(format!("Done: {score}"));
            Ok(score)
        }
        Ok(None) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            Err(error::system(
                "No score returned. Use case may not have any inputs",
                "",
            ))
        }
        Err(EvaluationError::Python(e)) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
            Err(error::user(
                "Failed to run pipeline",
                "Check the above error and try again",
            ))
        }
        Err(e) => {
            pipeline_pb.finish_with_message("Failed to run pipeline");
            Err(error::user(
                &format!("Failed to run pipeline: {e}"),
                "Check the pipeline configuration and try again",
            ))
        }
    };

    let mut file = std::fs::File::create(&last_run_result_file).map_err(|e| {
        error::user(
            &format!(
                "Failed to write to file {}: {}",
                last_run_result_file.display(),
                e
            ),
            &format!(
                "Make sure you have permissions to write to {}",
                last_run_result_file.display()
            ),
        )
    })?;
    if let Err(err) = rmp_serde::encode::write(
        &mut file,
        &LastRunResult {
            info: EvaluateAllInfo {
                score: if args.test.is_empty() {
                    result.as_ref().ok().cloned()
                } else {
                    None
                },
                num_inputs: num_inputs.load(std::sync::atomic::Ordering::Relaxed),
            },
            use_case_version: use_case_toml.version(),
            submission_version: project.version(),
        },
    ) {
        return Err(error::user(
            &format!(
                "Failed to write to file {}: {}",
                last_run_result_file.display(),
                err
            ),
            &format!(
                "Make sure you have permissions to write to {}",
                last_run_result_file.display()
            ),
        ));
    }

    result.map(|_| ())
}

pub async fn test(args: Test, global: GlobalArgs) -> Result<()> {
    let project = read_pyproject(&global.project).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;
    if aqora.is_submission() {
        test_submission(args, global, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run test on a submission instead",
        ))
    }
}
