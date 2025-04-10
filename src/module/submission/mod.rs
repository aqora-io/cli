mod experiment;
mod real;

use futures::FutureExt;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyDict};

// TODO: unify how we access experiments and real submissions?

static EXPERIMENT: tokio::sync::OnceCell<self::experiment::ExperimentSubmission> =
    tokio::sync::OnceCell::const_new();

#[pyfunction]
#[pyo3(signature = (index=0))]
pub fn get_input(py: Python<'_>, index: isize) -> PyResult<&PyAny> {
    let real = self::real::RealSubmission::global(py)?;

    pyo3_asyncio::tokio::future_into_py(py, async move {
        if let Some(real) = real {
            return Ok(real.get().input.clone());
        }

        let experiment = EXPERIMENT
            .get_or_try_init(self::experiment::ExperimentSubmission::load_notebook)
            .await?;
        experiment.load(index).await?;
        experiment.get_input()
    })
}

#[pyfunction]
#[pyo3(signature = (index=0))]
pub fn get_context(py: Python<'_>, index: isize) -> PyResult<&PyAny> {
    let real = self::real::RealSubmission::global(py)?;

    pyo3_asyncio::tokio::future_into_py(py, async move {
        if let Some(real) = real {
            return Ok(real.get().context.clone());
        }

        let experiment = EXPERIMENT
            .get_or_try_init(self::experiment::ExperimentSubmission::load_notebook)
            .await?;
        experiment.load(index).await?;
        experiment.get_context()
    })
}

#[pyfunction]
#[pyo3(signature = (output, index=0))]
pub fn set_output<'py>(py: Python<'py>, output: &'py PyAny, index: isize) -> PyResult<&'py PyAny> {
    let real = self::real::RealSubmission::global(py)?;
    let output = output.into_py(py);

    pyo3_asyncio::tokio::future_into_py(py, async move {
        if let Some(real) = real {
            return real
                .get()
                .output
                .send(output)
                .map_err(|_| PyErr::new::<PyRuntimeError, _>("set_output"));
        }

        let experiment = EXPERIMENT
            .get_or_try_init(self::experiment::ExperimentSubmission::load_notebook)
            .await?;
        experiment.load(index).await?;
        experiment.set_output(output);
        experiment.finish().await?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (submission, input, *, context=None, **_kwargs))]
pub fn _run_real_submission<'py>(
    py: Python<'py>,
    submission: &'py PyAny,
    input: &'py PyAny,
    context: Option<&'py PyAny>,
    _kwargs: Option<&'py PyAny>,
) -> PyResult<&'py PyAny> {
    let (tx, mut rx) = tokio::sync::watch::channel(py.None());
    let real = Py::new(
        py,
        self::real::RealSubmission {
            input: input.into_py(py),
            context: context.into_py(py),
            output: tx,
        },
    )?;
    let submission = submission.into_py(py);

    pyo3_asyncio::tokio::future_into_py(py, async move {
        // start submission
        let submission_completed = Python::with_gil(move |py| {
            let kwargs = PyDict::new(py);
            self::real::RealSubmission::set_global(real.borrow(py), kwargs)?;
            let ret = submission.into_ref(py).call((), Some(kwargs))?;
            let future = if isawaitable(ret)? {
                pyo3_asyncio::tokio::into_future(ret)?.boxed()
            } else {
                futures::future::ok(ret.into_py(py)).boxed()
            };
            PyResult::Ok(future)
        })?;

        // wait submission completion
        submission_completed.await?;
        rx.changed().await.unwrap();
        let output = rx.borrow().clone();

        // return output
        PyResult::Ok(output)
    })
}

fn isawaitable(any: &PyAny) -> PyResult<bool> {
    static DEF: pyo3::sync::GILOnceCell<PyObject> = pyo3::sync::GILOnceCell::new();

    let py = any.py();
    let def = DEF
        .get_or_try_init(py, || {
            PyResult::Ok(
                py.import(pyo3::intern!(py, "inspect"))?
                    .getattr(pyo3::intern!(py, "isawaitable"))?
                    .into(),
            )
        })?
        .as_ref(py);
    def.call1((any,))?.extract()
}
