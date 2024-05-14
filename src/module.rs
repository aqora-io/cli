use crate::commands::Cli;
use aqora_runner::pipeline::{LayerEvaluation, PipelineConfig};
use clap::Parser;
use pyo3::prelude::*;

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();
    let cli = Cli::parse_from(std::env::args().skip(1));
    cli.run(py).into()
}

#[pymodule]
pub fn aqora_cli(_: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    Ok(())
}
