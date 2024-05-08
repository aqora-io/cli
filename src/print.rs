use indicatif::ProgressBar;
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

#[pyclass]
struct ProgressPrint {
    progress: ProgressBar,
    old_print: PyObject,
}

#[pymethods]
impl ProgressPrint {
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python<'_>,
        args: &PyTuple,
        kwargs: Option<&PyDict>,
    ) -> PyResult<PyObject> {
        self.progress
            .suspend(|| self.old_print.call(py, args, kwargs))
    }
}

pub fn override_print(progress: ProgressBar) -> PyResult<()> {
    Python::with_gil(|py| {
        let builtins = py.import(pyo3::intern!(py, "builtins"))?;
        let print_name = pyo3::intern!(py, "print");
        let old_print = builtins.getattr(print_name)?.to_object(py);
        builtins.setattr(
            print_name,
            ProgressPrint {
                progress,
                old_print,
            }
            .into_py(py),
        )?;
        Ok(())
    })
}
