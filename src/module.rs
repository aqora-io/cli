use std::{
    collections::HashMap,
    ffi::OsString,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::mpsc::{channel, Sender},
};

use aqora_config::{PathStr, PyProject};
use aqora_runner::{
    pipeline::{LayerEvaluation, Pipeline, PipelineConfig},
    python::{PyEnv, PyEnvOptions},
};
use futures::TryStreamExt;
use pyo3::{
    exceptions::{PyIOError, PyRuntimeError, PyValueError},
    prelude::*,
    types::PyDict,
};

use crate::{
    dirs::{project_data_dir, project_use_case_toml_path, read_pyproject},
    run::tokio_runtime,
};

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();
    let sys = py.import("sys")?;
    let argv = sys.getattr("argv")?.extract::<Vec<OsString>>()?;
    let exit_code = py.allow_threads(|| crate::run(argv));
    sys.getattr("exit")?.call1((exit_code,))?;
    Ok(())
}

fn python_module_from_notebook_path(mut path: PathBuf) -> Option<PathStr<'static>> {
    if !path
        .extension()
        .is_some_and(|extension| extension == "ipynb")
    {
        return None;
    }
    path.set_extension("");
    Some(PathStr::new(
        path.components()
            .map(|x| x.as_os_str().to_string_lossy().to_string()),
    ))
}

fn ipython_notebook_module(
    py: Python<'_>,
    ipython: &PyObject,
    project_dir: impl AsRef<Path>,
) -> PyResult<Option<PathStr<'static>>> {
    let user_global_ns0 = ipython.getattr(py, "user_global_ns")?;
    let user_global_ns = user_global_ns0.downcast::<PyDict>(py)?;
    for file_key in ["__file__", "__vsc_ipynb_file__", "__session__"] {
        if let Some(file_path) = user_global_ns.get_item(file_key)? {
            let mut file_path = PathBuf::from(file_path.extract::<String>()?);
            if file_path.is_absolute() {
                file_path = file_path
                    .strip_prefix(project_dir)
                    .map_err(|error| PyErr::new::<PyIOError, _>(error.to_string()))?
                    .to_path_buf();
            }
            return Ok(python_module_from_notebook_path(file_path));
        }
    }
    Ok(None)
}

async fn get_submission_input(
    venv_path: PathBuf,
    project_dir: PathBuf,
    notebook_module: PathStr<'_>,
) -> PyResult<PyObject> {
    let submission_config = read_pyproject(&project_dir)
        .await
        .map_err(|error| PyErr::new::<PyRuntimeError, _>(error.description()))?;
    let submission = submission_config
        .aqora()
        .and_then(|x| x.as_submission())
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("expected project to be a submission"))?;

    let submission_layer = submission
        .refs
        .iter()
        .find_map(|(ref_name, ref_fn)| {
            if ref_fn.notebook && ref_fn.path == notebook_module {
                Some(ref_name.as_str())
            } else {
                None
            }
        })
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("notebook is not part of submission"))?;

    let use_case_toml_path = project_use_case_toml_path(&project_dir);
    let use_case_config =
        PyProject::from_toml(tokio::fs::read_to_string(use_case_toml_path).await?)
            .map_err(|error| PyErr::new::<PyValueError, _>(error.to_string()))?;
    let use_case = use_case_config
        .aqora()
        .and_then(|aqora| aqora.as_use_case())
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("expected project to be a use-case"))?;

    let uv_path =
        which::which("uv").map_err(|error| PyErr::new::<PyIOError, _>(error.to_string()))?;
    let env = PyEnv::init(uv_path, &venv_path, PyEnvOptions::default())
        .await
        .map_err(|error| PyErr::new::<PyRuntimeError, _>(error.to_string()))?;
    let data_path = project_data_dir(&project_dir);
    let pipeline = Pipeline::import(&env, use_case, PipelineConfig { data: data_path })?;

    let mut generator = Box::pin(pipeline.generator()?);
    let input = generator
        .try_next()
        .await?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("use-case yielded no input"))?;
    let input = pipeline
        .evaluator()
        .evaluate_until(submission_layer, input, None)
        .await
        .map_err(|error| PyErr::new::<PyRuntimeError, _>(error.to_string()))?;
    PyResult::Ok(input)
}

/// This function creates a new `threading.Thread`, then runs a async function in it.
/// This is only useful if you need to synchronously get the result of a asynchronous function
/// when an event loop is already running.
fn run_pythread<Fut>(py: Python<'_>, fut: Fut) -> PyResult<PyObject>
where
    Fut: Future<Output = PyResult<PyObject>> + Send + Sync + 'static,
{
    #[pyclass]
    struct Callback {
        sender: Sender<PyObject>,
        fut: Option<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send + Sync>>>,
    }

    #[pymethods]
    impl Callback {
        fn __call__(&mut self, py: Python<'_>) -> PyResult<()> {
            let Some(fut) = self.fut.take() else {
                return Ok(());
            };
            let result = pyo3_asyncio::tokio::run(py, fut)?;
            let _ = self.sender.send(result);
            Ok(())
        }
    }

    let (tx, rx) = channel();
    let kwargs = PyDict::new(py);
    kwargs.set_item(
        "target",
        Py::new(
            py,
            Callback {
                sender: tx,
                fut: Some(Box::pin(fut)),
            },
        )?,
    )?;

    let thread = py
        .import("threading")?
        .call_method("Thread", (), Some(kwargs))?;
    thread.call_method0("start")?;
    Ok(py.allow_threads(move || rx.recv().unwrap()))
}

#[pyfunction]
pub fn load_ipython_extension(py: Python<'_>, ipython: PyObject) -> PyResult<()> {
    let venv_path = PathBuf::from(py.import("sys")?.getattr("prefix")?.extract::<String>()?);
    let project_dir = venv_path
        .parent()
        .ok_or_else(|| PyErr::new::<PyIOError, _>("cannot access parent of virtual env directory"))?
        .to_path_buf();
    let notebook_module = ipython_notebook_module(py, &ipython, &project_dir)?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("notebook not found"))?;

    let tokio = tokio_runtime();
    pyo3_asyncio::tokio::init_with_runtime(tokio).unwrap();
    let input = if pyo3_asyncio::get_running_loop(py).is_ok() {
        run_pythread(
            py,
            get_submission_input(venv_path, project_dir, notebook_module),
        )?
    } else {
        pyo3_asyncio::tokio::run(
            py,
            get_submission_input(venv_path, project_dir, notebook_module),
        )?
    };

    let mut variables = HashMap::new();
    variables.insert("input", input);
    ipython.call_method1(py, "push", (variables,))?;

    Ok(())
}

#[pyfunction]
pub fn unload_ipython_extension(py: Python<'_>, _ipython: PyObject) -> PyResult<()> {
    py.eval("print('aqora unloaded')", None, None)?;
    Ok(())
}

#[pymodule]
pub fn aqora_cli(_: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_function(wrap_pyfunction!(load_ipython_extension, m)?)?;
    m.add_function(wrap_pyfunction!(unload_ipython_extension, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    Ok(())
}
