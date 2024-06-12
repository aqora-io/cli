use crate::error::{self, Result};
use indicatif::ProgressBar;
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
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
        args: &PyTuple,
        kwargs: Option<&PyDict>,
    ) -> PyResult<PyObject> {
        self.progress.suspend(|| self.func.call(py, args, kwargs))
    }
}

fn override_print(py: Python, progress: ProgressBar) -> PyResult<()> {
    let builtins = py.import(pyo3::intern!(py, "builtins"))?;
    let print_name = pyo3::intern!(py, "print");
    let old_print = builtins.getattr(print_name)?.to_object(py);
    builtins.setattr(
        print_name,
        ProgressSuspendPyFunc {
            progress,
            func: old_print,
        }
        .into_py(py),
    )?;
    Ok(())
}

fn override_os_system(py: Python, progress: ProgressBar) -> PyResult<()> {
    let os = py.import(pyo3::intern!(py, "os"))?;
    let system_name = pyo3::intern!(py, "system");
    let old_system = os.getattr(system_name)?.to_object(py);
    os.setattr(
        system_name,
        ProgressSuspendPyFunc {
            progress,
            func: old_system,
        }
        .into_py(py),
    )?;
    Ok(())
}

pub fn wrap_python_output(progress: &ProgressBar) -> Result<()> {
    Python::with_gil(|py| {
        override_print(py, progress.clone())?;
        override_os_system(py, progress.clone())?;
        PyResult::Ok(())
    })
    .map_err(|err| {
        progress.suspend(|| {
            Python::with_gil(|py| err.print_and_set_sys_last_vars(py));
        });
        error::system("Failed to set python hooks", "")
    })
}
