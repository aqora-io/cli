use pyo3::{prelude::*, types::PyDict, FromPyPointer as _};

#[pyclass(frozen)]
pub struct RealSubmission {
    pub input: PyObject,
    pub context: PyObject,
    pub output: tokio::sync::watch::Sender<PyObject>,
}

fn py_globals(py: Python<'_>) -> &PyAny {
    // SAFETY: GIL is acquired so PyEval_GetGlobals() won't return NULL
    unsafe { PyAny::from_borrowed_ptr(py, pyo3::ffi::PyEval_GetGlobals()) }
}

impl RealSubmission {
    const ATTR_NAME: &str = "__aqora_submission_handler";

    pub fn global(py: Python<'_>) -> PyResult<Option<Py<Self>>> {
        let globals = py_globals(py).downcast::<PyDict>()?;
        match globals.get_item(Self::ATTR_NAME)? {
            Some(any) => Ok(Some(any.downcast_exact::<PyCell<Self>>()?.into())),
            None => Ok(None),
        }
    }

    pub fn set_global<'py>(this: PyRef<'py, Self>, global: &'py PyDict) -> PyResult<()> {
        global.set_item(Self::ATTR_NAME, this.into_py(global.py()))?;
        Ok(())
    }
}
