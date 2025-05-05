mod submission;

use std::ffi::OsString;

use aqora_runner::pipeline::{LayerEvaluation, PipelineConfig};
use pyo3::prelude::*;

#[pymodule]
pub fn aqora_cli(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_function(wrap_pyfunction!(submission::get_input, m)?)?;
    m.add_function(wrap_pyfunction!(submission::get_context, m)?)?;
    m.add_function(wrap_pyfunction!(submission::set_output, m)?)?;
    m.add_function(wrap_pyfunction!(submission::_run_real_submission, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    Ok(())
}

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();
    let sys = py.import("sys")?;
    let argv = sys.getattr("argv")?.extract::<Vec<OsString>>()?;
    let exit_code = py.allow_threads(|| crate::run(argv));
    sys.getattr("exit")?.call1((exit_code,))?;
    Ok(())
}
