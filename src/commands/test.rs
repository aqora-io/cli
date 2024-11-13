use crate::{
    commands::GlobalArgs,
    config::read_project_config,
    dirs::{
        project_data_dir, project_last_run_dir, project_last_run_result,
        project_use_case_toml_path, read_pyproject,
    },
    error::{self, Result},
    evaluate::evaluate,
    ipynb::{convert_submission_notebooks, convert_use_case_notebooks},
    print::wrap_python_output,
    python::LastRunResult,
};
use aqora_config::{AqoraUseCaseConfig, PyProject};
use aqora_runner::{
    pipeline::{EvaluateAllInfo, EvaluateInputInfo, EvaluationError, Pipeline, PipelineConfig},
    python::PyEnv,
};
use clap::Args;
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar};
use owo_colors::{OwoColorize, Stream as OwoStream};
use pyo3::prelude::*;
use pyo3::{exceptions::PyException, Python};
use serde::Serialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{atomic::AtomicU32, Arc},
};

#[derive(Args, Debug, Clone, Serialize)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long)]
    pub test: Vec<String>,
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

struct RunPipelineConfig {
    use_case: AqoraUseCaseConfig,
    pipeline_config: PipelineConfig,
    last_run_dir: PathBuf,
    tests: Vec<usize>,
    max_concurrency: usize,
}

async fn do_run_pipeline(
    env: PyEnv,
    config: RunPipelineConfig,
    name: Option<String>,
    pb: ProgressBar,
) -> Result<(u32, Result<Option<PyObject>, EvaluationError>)> {
    let label = name
        .as_ref()
        .map(|name| format!(" for {name}"))
        .unwrap_or_default();

    pb.set_message("Importing pipeline..");

    let pipeline = match Pipeline::import(&env, &config.use_case, config.pipeline_config) {
        Ok(pipeline) => pipeline,
        Err(err) => {
            pb.suspend(|| {
                Python::with_gil(|py| err.print_and_set_sys_last_vars(py));
            });
            pb.finish_with_message(format!("Failed to import pipeline{label}"));
            return Err(error::user(
                "Failed to import pipeline",
                "Check the above error and try again",
            ));
        }
    };

    pb.set_message("Running tests...");

    let (num_inputs, generator) = if config.tests.is_empty() {
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
                pb.suspend(|| {
                    Python::with_gil(|py| error.print_and_set_sys_last_vars(py));
                });
                pb.finish_with_message(format!("Failed to run pipeline{label}"));
                return Err(error::user(
                    &format!("Unable to generate an inputs{label}"),
                    "Check the above error and try again",
                ));
            }
        }
    } else {
        let inputs = last_run_items(&config.last_run_dir, config.tests)
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
                    &format!("Failed to read last run data{label} for {index}: {err}"),
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

    let aggregated = pipeline
        .aggregate(evaluate(
            pipeline.evaluator(),
            generator,
            config.max_concurrency,
            Some(config.last_run_dir),
            name.map(|name| name.to_string()),
            pb.clone(),
        ))
        .await;

    Ok((
        num_inputs.load(std::sync::atomic::Ordering::Relaxed),
        aggregated,
    ))
}

fn run_pipeline(
    env: &PyEnv,
    config: RunPipelineConfig,
    name: Option<&str>,
    pb: &ProgressBar,
) -> Result<(u32, Result<Option<PyObject>, EvaluationError>)> {
    let label = name
        .as_ref()
        .map(|name| format!(" for {name}"))
        .unwrap_or_default();
    let run_pb = pb.clone();
    let run_env = env.clone();
    let run_name = name.map(|n| n.to_string());
    Ok(
        match pyo3::Python::with_gil(move |py| {
            pyo3_asyncio::tokio::run(py, async move {
                Ok(do_run_pipeline(run_env, config, run_name, run_pb).await)
            })
        }) {
            Ok(res) => res?,
            Err(err) => {
                pb.suspend(|| {
                    Python::with_gil(|py| err.print_and_set_sys_last_vars(py));
                });
                pb.finish_with_message(format!("Failed to run pipeline{label}"));
                return Err(error::system(
                    &format!("Failed to run pipeline{label}"),
                    "Check the above error and try again",
                ));
            }
        },
    )
}

pub async fn run_submission_tests(
    m: &MultiProgress,
    global: &GlobalArgs,
    project: &PyProject,
    tests: Vec<String>,
) -> Result<()> {
    let submission = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| error::user("Submission config is not valid", ""))?;

    let project_config = read_project_config(&global.project).await?;

    let use_case_toml_path = project_use_case_toml_path(&global.project);
    let data_path = project_data_dir(&global.project);
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
    let last_run_result_file = project_last_run_result(&global.project);
    if tests.is_empty() {
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

    let mut pipeline_pb = ProgressBar::new_spinner().with_message("Starting pipeline...");
    pipeline_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pipeline_pb = m.add(pipeline_pb);

    pipeline_pb.set_message("Setting up virtual environment...");

    let env = global.init_venv(&pipeline_pb).await?;

    pipeline_pb.set_message("Converting notebooks...");

    let modified_use_case = {
        let mut use_case = use_case.clone();
        let mut submission = submission.clone();
        convert_submission_notebooks(&env, &mut submission).await?;
        if let Err(err) = use_case.replace_refs(&submission.refs) {
            return Err(error::system(
                &format!("Failed to import pipeline: {err}"),
                "Check the pipeline configuration and try again",
            ));
        }
        use_case
    };
    let config = PipelineConfig {
        data: dunce::canonicalize(data_path)?,
    };

    wrap_python_output(&pipeline_pb)?;

    let tests = tests
        .iter()
        .map(|test| {
            test.parse::<usize>().map_err(|_| {
                error::user(
                    &format!("Invalid test index: {test}"),
                    "Please provide a valid test index",
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let (num_inputs, aggregated) = run_pipeline(
        &env,
        RunPipelineConfig {
            use_case: modified_use_case,
            pipeline_config: config,
            tests: tests.clone(),
            last_run_dir,
            max_concurrency: global.max_concurrency,
        },
        None,
        &pipeline_pb,
    )?;

    let result = match aggregated {
        Ok(Some(score)) => {
            if project_config.show_score {
                pipeline_pb.println(format!(
                    "{}: {}",
                    "Score".if_supports_color(OwoStream::Stdout, |text| { text.bold() }),
                    score
                ));
            }
            pipeline_pb.finish_and_clear();
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
            pipeline_pb.suspend(|| {
                Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
            });
            pipeline_pb.finish_with_message("Failed to run pipeline");
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
                score: if tests.is_empty() {
                    result.as_ref().ok().cloned()
                } else {
                    None
                },
                num_inputs,
            },
            time: chrono::Utc::now(),
            use_case_version: use_case_toml.version(),
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

pub async fn test_submission(args: Test, global: GlobalArgs, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();
    run_submission_tests(&m, &global, &project, args.test).await
}

async fn test_use_case_test(
    m: &MultiProgress,
    env: &PyEnv,
    max_concurrency: usize,
    last_run_dir: &Path,
    use_case: &AqoraUseCaseConfig,
    name: &str,
    indexes: Vec<usize>,
) -> Result<()> {
    let pb = m.insert_from_back(
        1,
        ProgressBar::new_spinner().with_message(format!("Running test {name}...")),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let modified_use_case = use_case.for_test(name).map_err(|err| {
        pb.finish_with_message("Failed to run pipeline for {name}");
        error::user(
            &format!("Failed to load test config: {err}"),
            "Check the pipeline configuration and try again",
        )
    })?;

    let last_run_dir = last_run_dir.join(name);
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

    let config = PipelineConfig {
        data: modified_use_case.data.clone(),
    };

    let (_, aggregated) = run_pipeline(
        env,
        RunPipelineConfig {
            use_case: modified_use_case,
            pipeline_config: config,
            tests: indexes.clone(),
            last_run_dir,
            max_concurrency,
        },
        Some(name),
        &pb,
    )?;

    let result = match aggregated {
        Ok(Some(score)) => score,
        Ok(None) => {
            pb.finish_with_message(format!("Failed to run pipeline for {name}"));
            return Err(error::system(
                &format!("No score returned for {name}. Use case may not have any inputs"),
                "",
            ));
        }
        Err(EvaluationError::Python(e)) => {
            pb.suspend(|| {
                Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
            });
            pb.finish_with_message(format!("Failed to run pipeline for {name}"));
            return Err(error::user(
                &format!("Failed to run pipeline for {name}"),
                "Check the above error and try again",
            ));
        }
        Err(e) => {
            pb.finish_with_message(format!("Failed to run pipeline for {name}"));
            return Err(error::user(
                &format!("Failed to run pipeline for {name}: {e}"),
                "Check the pipeline configuration and try again",
            ));
        }
    };

    if !indexes.is_empty() {
        if let Some(expected) = use_case
            .tests
            .get(name)
            .and_then(|test| test.expected.as_ref())
        {
            let expected_json: serde_json::Value = serde_json::to_string(expected)
                .and_then(|s| serde_json::from_str(&s))
                .map_err(|e| {
                    pb.finish_with_message(format!("Failed to run pipeline for {name}"));
                    error::user(
                        &format!("Failed to convert {name} expected score to JSON: {e}"),
                        "Check the pipeline configuration and try again",
                    )
                })?;

            let score_json: serde_json::Value = Python::with_gil(|py| {
                py.import("ujson")?
                    .getattr("dumps")?
                    .call1((result.clone(),))?
                    .extract::<String>()
            })
            .map_err(|e| {
                pb.suspend(|| {
                    Python::with_gil(|py| e.print_and_set_sys_last_vars(py));
                });
                pb.finish_with_message(format!("Failed to evaluate {name} score"));
                error::user(
                    &format!("Failed to convert {name} score to JSON"),
                    "Check the pipeline configuration and try again",
                )
            })
            .and_then(|s| {
                serde_json::from_str(&s).map_err(|e| {
                    pb.finish_with_message(format!("Failed to evaluate {name} score"));
                    error::user(
                        &format!("Failed to convert {name} score to JSON: {e}"),
                        "Check the pipeline configuration and try again",
                    )
                })
            })?;

            if expected_json != score_json {
                pb.finish_with_message(format!("{name} score does not match the expected score"));
                return Err(error::user(
                &format!(
                    "Expected score for {name} does not match the actual score: {expected} != {result}",
                    expected = expected_json,
                    result = score_json
                ),
                "Check the pipeline configuration and try again",
            ));
            }
        }
    }

    pb.finish_with_message(format!("Test {name} passed: {result}"));

    Ok(())
}

async fn test_use_case(args: Test, global: GlobalArgs, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();
    let use_case = project
        .aqora()
        .and_then(|aqora| aqora.as_use_case())
        .ok_or_else(|| error::user("Use case config is not valid", ""))?;
    let tests: HashMap<String, Option<Vec<usize>>> = if args.test.is_empty() {
        use_case
            .tests
            .keys()
            .map(|name| (name.to_string(), None))
            .collect()
    } else {
        args.test.iter().try_fold::<_, _, Result<_>>(
            HashMap::<String, Option<Vec<usize>>>::new(),
            |mut acc, test| {
                let (name, index) = if let Some((name, index)) = test.rsplit_once("::") {
                    (
                        name,
                        Some(index.parse::<usize>().map_err(|_| {
                            error::user(
                                &format!("Invalid test index for {name}: {index}"),
                                "Please provide a valid test index",
                            )
                        })?),
                    )
                } else {
                    (test.as_str(), None)
                };
                if let Some(maybe_indexes) = acc.get_mut(name) {
                    if let Some(indexes) = maybe_indexes {
                        if let Some(index) = index {
                            indexes.push(index)
                        } else {
                            *maybe_indexes = None;
                        }
                    }
                } else if use_case.tests.contains_key(name) {
                    acc.insert(name.to_string(), index.map(|i| vec![i]));
                } else {
                    return Err(error::user(
                        &format!("Test {name} not found"),
                        "Please provide a valid test name",
                    ));
                }
                Ok(acc)
            },
        )?
    };

    let venv_pb =
        m.add(ProgressBar::new_spinner().with_message("Setting up virtual environment..."));
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let env = global.init_venv(&venv_pb).await?;

    let mut use_case = use_case.clone();
    convert_use_case_notebooks(&env, &mut use_case).await?;

    venv_pb.finish_with_message("Virtual environment ready");

    let test_pb = m.add(ProgressBar::new_spinner().with_message("Running tests..."));
    test_pb.enable_steady_tick(std::time::Duration::from_millis(100));

    wrap_python_output(&test_pb)?;

    let last_run_dir = project_last_run_dir(&global.project);
    for (name, indexes) in tests {
        let indexes = indexes.unwrap_or_default();
        test_use_case_test(
            &m,
            &env,
            global.max_concurrency,
            &last_run_dir,
            &use_case,
            &name,
            indexes,
        )
        .await
        .inspect_err(|_| test_pb.finish_with_message("Failed to run tests"))?;
    }

    test_pb.finish_with_message("All tests passed!");

    Ok(())
}

pub async fn test(args: Test, global: GlobalArgs) -> Result<()> {
    let project = read_pyproject(&global.project).await?;
    let aqora = project.aqora().cloned().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;

    if aqora.is_submission() {
        test_submission(args, global, PyProject::clone(&project)).await?;
    } else {
        test_use_case(args, global, PyProject::clone(&project)).await?;
    };

    Ok(())
}
