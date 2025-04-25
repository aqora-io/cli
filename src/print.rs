use crate::error::{self, Result};
use indicatif::ProgressBar;
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule, PyString, PyTuple},
};

#[pyclass]
struct ProgressSuspendPyFunc {
    progress: ProgressBar,
    func: PyObject,
}

#[pymethods]
impl ProgressSuspendPyFunc {
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        self.progress.suspend(|| self.func.call(py, args, kwargs))
    }

    fn __getattr__(&self, py: Python<'_>, name: &Bound<'_, PyString>) -> PyResult<PyObject> {
        self.func.getattr(py, name)
    }

    fn __setattr__(
        &self,
        py: Python<'_>,
        name: &Bound<'_, PyString>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.func.setattr(py, name, value)
    }

    fn __delattr__(&self, py: Python<'_>, name: &Bound<'_, PyString>) -> PyResult<()> {
        self.func.bind_borrowed(py).delattr(name)
    }
}

fn override_module_func(
    py: Python,
    module: &Bound<'_, PyModule>,
    name: &Bound<'_, PyString>,
    progress: ProgressBar,
) -> PyResult<()> {
    let old_func = module.getattr(name)?.into_pyobject(py)?;

    module.setattr(
        name,
        ProgressSuspendPyFunc {
            progress,
            func: old_func.unbind(),
        },
    )?;

    Ok(())
}

pub fn wrap_python_output(progress: &ProgressBar) -> Result<()> {
    Python::with_gil(|py| {
        override_module_func(
            py,
            &py.import(pyo3::intern!(py, "builtins"))?,
            pyo3::intern!(py, "print"),
            progress.clone(),
        )?;
        override_module_func(
            py,
            &py.import(pyo3::intern!(py, "os"))?,
            pyo3::intern!(py, "system"),
            progress.clone(),
        )?;
        PyResult::Ok(())
    })
    .map_err(|err| {
        progress.suspend(|| {
            Python::with_gil(|py| err.print_and_set_sys_last_vars(py));
        });
        error::system("Failed to set python hooks", "")
    })
}
