use crate::error::{self, Result};
use indicatif::ProgressBar;
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule, PyString, PyTuple},
};
use std::cell::Cell;

thread_local! {
    static SUSPENDED: Cell<bool> = const { Cell::new(false) };
}

/// [`ProgressBar::suspend`] that can be nested: indicatif deadlocks when a thread suspends a
/// bar while it is already suspending one of the same `MultiProgress`, which happens when a
/// Python error is printed through the `print` installed by [`wrap_python_output`].
pub fn suspend<R>(progress: &ProgressBar, f: impl FnOnce() -> R) -> R {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            SUSPENDED.set(false);
        }
    }
    if SUSPENDED.get() {
        return f();
    }
    SUSPENDED.set(true);
    let _reset = Reset;
    progress.suspend(f)
}

#[pyclass]
struct ProgressSuspendPyFunc {
    progress: ProgressBar,
    func: Py<PyAny>,
}

#[pymethods]
impl ProgressSuspendPyFunc {
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        suspend(&self.progress, || self.func.call(py, args, kwargs))
    }

    fn __getattr__(&self, py: Python<'_>, name: &Bound<'_, PyString>) -> PyResult<Py<PyAny>> {
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
    Python::attach(|py| {
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
        suspend(progress, || {
            Python::attach(|py| err.print_and_set_sys_last_vars(py));
        });
        error::system("Failed to set python hooks", "")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use indicatif::{MultiProgress, ProgressDrawTarget};
    use std::{io::Write, sync::mpsc, thread, time::Duration};

    #[test]
    fn test_print_while_suspended() {
        pyo3::Python::initialize();
        let m = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let outer = m.add(ProgressBar::new_spinner());
        let inner = m.add(ProgressBar::new_spinner());
        wrap_python_output(&inner).unwrap();
        let (tx, rx) = mpsc::channel();
        // On another thread so that a deadlock fails the test instead of hanging it
        thread::spawn(move || {
            let res = suspend(&outer, || {
                Python::attach(|py| {
                    py.run(c"import io; print('hi', file=io.StringIO())", None, None)
                })
            });
            tx.send(res.is_ok()).unwrap();
        });
        let ok = rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| {
                // The deadlocked thread holds the GIL and would hang every other Python test, so
                // end the run. Write to stderr directly: captured test output is lost on exit.
                let _ = writeln!(std::io::stderr(), "print deadlocked inside suspend");
                std::process::exit(1)
            });
        assert!(ok);
    }

    #[test]
    fn test_suspend_resets_after_panic() {
        let pb = ProgressBar::hidden();
        let res = std::panic::catch_unwind(|| suspend(&pb, || panic!("boom")));
        assert!(res.is_err());
        assert!(!SUSPENDED.get());
    }
}
