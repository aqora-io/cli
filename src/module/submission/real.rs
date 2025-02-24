use pyo3::{prelude::*, types::PyDict};

#[pyclass(frozen)]
pub struct RealSubmission {
    pub input: PyObject,
    pub context: PyObject,
    pub output: tokio::sync::watch::Sender<PyObject>,
}

fn py_globals(py: Python<'_>) -> Bound<'_, PyAny> {
    // SAFETY: GIL is acquired so PyEval_GetGlobals() won't return NULL
    unsafe { Bound::from_borrowed_ptr(py, pyo3::ffi::PyEval_GetGlobals()) }
}

impl RealSubmission {
    const ATTR_NAME: &str = "__aqora_submission_handler";

    pub fn global(py: Python<'_>) -> PyResult<Option<Py<Self>>> {
        let globals = py_globals(py).downcast_into::<PyDict>()?;
        match globals.get_item(Self::ATTR_NAME)? {
            Some(any) => Ok(Some(any.downcast_into_exact::<Self>()?.unbind())),
            None => Ok(None),
        }
    }

    pub fn set_global<'py>(this: PyRef<'py, Self>, global: Bound<'py, PyDict>) -> PyResult<()> {
        global.set_item(Self::ATTR_NAME, this)?;
        Ok(())
    }
}
