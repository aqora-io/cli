use pyo3::prelude::*;

#[pyclass]
struct IPython;

#[pymethods]
impl IPython {
    fn system(&self, py: Python<'_>, code: &str) -> PyResult<()> {
        let os = py.import(pyo3::intern!(py, "os"))?;
        os.getattr("system")?.call1((code,))?;
        Ok(())
    }

    fn __getattr__(&self, name: &str) -> PyResult<PyObject> {
        Err(pyo3::exceptions::PyAttributeError::new_err(format!(
            "aqora's 'ipython' does not support '{name}'",
        )))
    }
}

#[pyclass]
struct GetIPython;

#[pymethods]
impl GetIPython {
    fn __call__(&self) -> IPython {
        IPython
    }
}

pub fn override_get_ipython(py: Python<'_>) -> PyResult<()> {
    py.import(pyo3::intern!(py, "builtins"))?
        .setattr(pyo3::intern!(py, "get_ipython"), GetIPython.into_py(py))
}
