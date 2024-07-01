use crate::error::Result;
use aqora_runner::pipeline::{EvaluateInputInfo, EvaluationError, EvaluationResult, Evaluator};
use clap::Args;
use futures::prelude::*;
use indicatif::ProgressBar;
use owo_colors::{OwoColorize, Stream as OwoStream, Style};
use pyo3::prelude::*;
use std::{path::PathBuf, sync::Arc};

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long)]
    pub test: Vec<usize>,
}

pub fn evaluate(
    evaluator: Evaluator,
    inputs: impl Stream<Item = (usize, PyResult<PyObject>)>,
    concurrency: usize,
    last_run_dir: Option<PathBuf>,
    label: Option<String>,
    pb: ProgressBar,
) -> impl Stream<Item = Result<EvaluationResult, (EvaluationResult, EvaluationError)>> {
    let evaluator = Arc::new(evaluator);
    inputs
        .map(move |input| (input, evaluator.clone()))
        .map(|((index, result), evaluator)| async move {
            match result {
                Ok(input) => match evaluator.evaluate(input.clone(), None).await {
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
        .buffer_unordered(concurrency)
        .map(move |(index, item)| {
            let label = if let Some(label) = label.as_ref() {
                format!("{}::{}", label, index + 1)
            } else {
                format!("{}", index + 1)
            };
            (index, item, last_run_dir.clone(), label, pb.clone())
        })
        .then(|(index, item, last_run_dir, label, pb)| async move {
            if let Some(last_run_dir) = last_run_dir {
                let filename = last_run_dir.join(format!("{index}.msgpack"));
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
                        format!("[{label} ERR]")
                            .if_supports_color(OwoStream::Stdout, |text| text.red()),
                        err = err
                    ));
                    return Err((item.result, EvaluationError::custom(err)));
                }
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
                format!("[{label} {}]", if is_ok { "OK" } else { "FAIL" }).if_supports_color(
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
