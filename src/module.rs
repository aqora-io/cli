use std::ffi::OsString;

use aqora_runner::pipeline::{LayerEvaluation, PipelineConfig};
use pyo3::prelude::*;

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();

    let argv = py
        .import("sys")?
        .getattr("argv")?
        .extract::<Vec<OsString>>()?;

    crate::run(argv);
    Ok(())
}

#[pymodule]
pub fn aqora_cli(_: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    Ok(())
}
