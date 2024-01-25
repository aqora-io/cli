use crate::error::Result;
use pyo3::prelude::*;
use std::path::PathBuf;

pub fn get_python_executable() -> Result<PathBuf> {
    Python::with_gil(|py| {
        let sys = py.import("sys")?;
        let executable: String = sys
            .getattr(pyo3::intern!(sys.py(), "executable"))?
            .extract()?;
        Ok(PathBuf::from(executable))
    })
}
