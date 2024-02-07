use futures::prelude::*;
use pyo3::{prelude::*, pyclass::IterANextOutput, types::PyType};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::{process::Command, sync::RwLock};

lazy_static::lazy_static! {
    static ref SYSTEM_PYTHON_PATH: PathBuf = {
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let executable: String = sys
                .getattr(pyo3::intern!(py, "executable")).unwrap()
                .extract().unwrap();
            PathBuf::from(executable)
        })
    };
    static ref INITIALIZED_ENVS: RwLock<HashSet<PathBuf>> = RwLock::new(HashSet::new());
}

#[derive(Default)]
pub struct PipOptions {
    pub upgrade: bool,
    pub no_deps: bool,
}

pub struct PyEnv(PathBuf);

#[derive(Error, Debug)]
pub enum EnvError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error(transparent)]
    Python(#[from] PyErr),
    #[error("Failed to setup virtualenv: {0}")]
    VenvFailed(String),
}

impl PyEnv {
    pub async fn init(path: impl AsRef<Path>) -> Result<Self, EnvError> {
        Self::ensure_venv(&path).await?;
        let path = path.as_ref().canonicalize()?;
        if INITIALIZED_ENVS.read().await.contains(&path) {
            return Ok(Self(path));
        }
        let mut lib_dir_entries = tokio::fs::read_dir(path.join("lib")).await?;
        while let Some(entry) = lib_dir_entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let name = entry.file_name();
                if let Some(name) = name.to_str() {
                    if name.starts_with("python") {
                        let site_packages = entry.path().join("site-packages");
                        if site_packages.exists() {
                            Python::with_gil(|py| {
                                let sys = py.import("sys").unwrap();
                                sys.getattr(pyo3::intern!(sys.py(), "path"))?
                                    .getattr(pyo3::intern!(sys.py(), "append"))?
                                    .call1((site_packages,))?;
                                PyResult::Ok(())
                            })?;
                        }
                    }
                }
            }
        }
        INITIALIZED_ENVS.write().await.insert(path.clone());
        Ok(Self(path))
    }

    pub async fn clear(self) -> Result<(), EnvError> {
        let mut envs = INITIALIZED_ENVS.write().await;
        Python::with_gil(|py| {
            py.run(
                r#"
import sys
this = sys.modules[__name__]
for n in dir():
    if n[0]!='_': delattr(this, n)
"#,
                None,
                None,
            )?;
            PyResult::Ok(())
        })?;
        envs.clear();
        Ok(())
    }

    async fn ensure_venv(path: impl AsRef<Path>) -> Result<(), EnvError> {
        let path = path.as_ref();
        if path.exists() && path.is_dir() {
            return Ok(());
        }
        let output = Command::new(SYSTEM_PYTHON_PATH.as_os_str())
            .arg("-m")
            .arg("venv")
            .arg(path)
            .output()
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(EnvError::VenvFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    pub fn python_path(&self) -> PathBuf {
        self.0.join("bin").join("python")
    }

    pub fn activate_path(&self) -> PathBuf {
        self.0.join("bin").join("activate")
    }

    pub fn python_cmd(&self) -> Command {
        Command::new(self.python_path().as_os_str())
    }

    pub async fn is_module_installed(&self, module: impl AsRef<OsStr>) -> tokio::io::Result<bool> {
        Ok(Command::new(self.python_path().as_os_str())
            .arg("-m")
            .arg(module)
            .output()
            .await?
            .status
            .success())
    }

    pub fn pip_install(
        &self,
        modules: impl IntoIterator<Item = impl AsRef<OsStr>>,
        opts: &PipOptions,
    ) -> Command {
        let mut cmd = self.python_cmd();
        cmd.arg("-m").arg("pip").arg("install");
        if opts.upgrade {
            cmd.arg("--upgrade");
        }
        if opts.no_deps {
            cmd.arg("--no-deps");
        }
        for module in modules {
            cmd.arg(module.as_ref());
        }
        cmd
    }

    pub fn build_package(&self, input: impl AsRef<Path>, output: impl AsRef<Path>) -> Command {
        let mut cmd = self.python_cmd();
        cmd.arg("-m")
            .arg("build")
            .arg("--sdist")
            .arg("--outdir")
            .arg(output.as_ref().as_os_str())
            .arg(input.as_ref().as_os_str());
        cmd
    }
}

macro_rules! async_python_run {
    ($($closure:tt)*) => {
        Python::with_gil(|py| {
            let closure = $($closure)*;
            let awaitable = match closure(py) {
                Ok(awaitable) => awaitable,
                Err(err) => return Err(err),
            };
            pyo3_asyncio::into_future_with_locals(
                &pyo3_asyncio::tokio::get_current_locals(py)?,
                awaitable,
            )
        })
    };
}
pub(crate) use async_python_run;

pub fn async_generator(generator: PyObject) -> PyResult<impl Stream<Item = PyResult<PyObject>>> {
    let generator = Python::with_gil(move |py| {
        generator
            .as_ref(py)
            .call_method0(pyo3::intern!(py, "__aiter__"))?;
        PyResult::Ok(generator)
    })?;
    Ok(
        futures::stream::unfold(generator, move |generator| async move {
            let result = match Python::with_gil(|py| {
                pyo3_asyncio::into_future_with_locals(
                    &pyo3_asyncio::tokio::get_current_locals(py)?,
                    generator
                        .as_ref(py)
                        .call_method0(pyo3::intern!(py, "__anext__"))?,
                )
            }) {
                Ok(result) => result.await,
                Err(err) => return Some((Err(err), generator)),
            };
            Python::with_gil(|py| match result {
                Ok(result) => Some((Ok(result), generator)),
                Err(err) => {
                    if err
                        .get_type(py)
                        .is(PyType::new::<pyo3::exceptions::PyStopAsyncIteration>(py))
                    {
                        None
                    } else {
                        Some((Err(err), generator))
                    }
                }
            })
        })
        .fuse(),
    )
}

pub fn deepcopy(obj: &PyObject) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let copy = py.import("copy")?.getattr("deepcopy")?;
        Ok(copy.call1((obj,))?.into_py(py))
    })
}

type AsyncIteratorStream = futures::stream::BoxStream<'static, PyObject>;

#[pyclass]
pub struct AsyncIterator {
    stream: std::sync::Arc<std::sync::Mutex<Option<AsyncIteratorStream>>>,
}

impl AsyncIterator {
    pub fn new(stream: impl Stream<Item = PyObject> + Send + Sync + 'static) -> Self {
        Self {
            stream: std::sync::Arc::new(std::sync::Mutex::new(Some(stream.boxed()))),
        }
    }
}

#[pymethods]
impl AsyncIterator {
    fn __aiter__(&self) -> PyResult<AsyncIteratorImpl> {
        Ok(AsyncIteratorImpl {
            stream: std::sync::Arc::new(tokio::sync::Mutex::new(
                self.stream
                    .lock()
                    .map_err(|err| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
                    })?
                    .take()
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                            "AsyncIterator already consumed",
                        )
                    })?,
            )),
        })
    }
}

#[pyclass]
struct AsyncIteratorImpl {
    stream: std::sync::Arc<tokio::sync::Mutex<AsyncIteratorStream>>,
}

#[pymethods]
impl AsyncIteratorImpl {
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<IterANextOutput<&'py PyAny, &'py PyAny>> {
        let stream = self.stream.clone();
        let result = pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                match stream.lock().await.next().await {
                    Some(value) => Ok(value),
                    None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
                }
            },
        )?;
        Ok(IterANextOutput::Yield(result))
    }
}
