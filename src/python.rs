use crate::error::{self, Result};
use crate::pyproject::project_data_dir;
use futures::prelude::*;
use indicatif::ProgressBar;
use pyo3::{prelude::*, pyclass::IterANextOutput, types::PyType};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::RwLock,
};
use url::Url;

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

pub fn venv_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_data_dir(project_dir).join("venv")
}

pub fn pypi_url(url: &Url, access_token: Option<impl AsRef<str>>) -> Result<Url> {
    let mut url = url.join("/pypi")?;
    if let Some(access_token) = access_token {
        url.set_username(access_token.as_ref())
            .map_err(|_| error::system("Could not set pypi access token", ""))?;
    }
    Ok(url)
}

#[derive(Default)]
pub struct PipOptions {
    pub upgrade: bool,
    pub no_deps: bool,
    pub editable: bool,
    pub extra_index_urls: Vec<Url>,
}

pub struct PyEnv(PathBuf);

impl PyEnv {
    pub async fn init(project_dir: impl AsRef<Path>) -> Result<Self> {
        let path = Self::ensure_venv(project_dir).await?.canonicalize()?;
        if INITIALIZED_ENVS.read().await.contains(&path) {
            return Ok(Self(path));
        }
        let site_packages = std::fs::read_dir(path.join("lib"))?
            .flatten()
            .filter(|entry| {
                entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false)
                    && entry
                        .file_name()
                        .to_str()
                        .map(|name| name.starts_with("python"))
                        .unwrap_or(false)
                    && std::fs::read_dir(entry.path())
                        .map(|dir| {
                            dir.flatten().any(|entry| {
                                entry
                                    .file_name()
                                    .to_str()
                                    .map(|name| name.starts_with("site-packages"))
                                    .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
            })
            .map(|path| path.path().join("site-packages"))
            .collect::<Vec<_>>();
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            for path in site_packages {
                sys.getattr(pyo3::intern!(sys.py(), "path"))?
                    .getattr(pyo3::intern!(sys.py(), "append"))?
                    .call1((path,))?;
            }
            PyResult::Ok(())
        })?;
        INITIALIZED_ENVS.write().await.insert(path.clone());
        Ok(Self(path))
    }

    async fn ensure_venv(project_dir: impl AsRef<Path>) -> Result<PathBuf> {
        let path = venv_dir(project_dir);
        if path.exists() && path.is_dir() {
            return Ok(path);
        }
        let output = tokio::process::Command::new(SYSTEM_PYTHON_PATH.as_os_str())
            .arg("-m")
            .arg("venv")
            .arg(&path)
            .output()
            .await?;
        if output.status.success() {
            Ok(path)
        } else {
            Err(error::user(
                &format!(
                    "Could not setup virtualenv: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
                "",
            ))
        }
    }

    pub fn python_path(&self) -> PathBuf {
        self.0.join("bin").join("python")
    }

    pub fn activate_path(&self) -> PathBuf {
        self.0.join("bin").join("activate")
    }

    pub fn python_cmd(&self) -> tokio::process::Command {
        tokio::process::Command::new(self.python_path().as_os_str())
    }

    async fn is_module_installed(&self, module: &str) -> Result<bool> {
        Ok(tokio::process::Command::new(self.python_path().as_os_str())
            .arg("-m")
            .arg(module)
            .output()
            .await?
            .status
            .success())
    }

    pub async fn pip_install(
        &self,
        modules: impl IntoIterator<Item = impl AsRef<str>>,
        opts: &PipOptions,
        progress: Option<&ProgressBar>,
    ) -> Result<()> {
        let mut cmd = self.python_cmd();
        cmd.arg("-m").arg("pip").arg("install");
        if opts.upgrade {
            cmd.arg("--upgrade");
        }
        for extra_index_url in &opts.extra_index_urls {
            cmd.arg("--extra-index-url")
                .arg(extra_index_url.to_string());
        }
        if opts.no_deps {
            cmd.arg("--no-deps");
        }
        if opts.editable {
            cmd.arg("--editable");
        }
        for module in modules {
            cmd.arg(module.as_ref());
        }
        let mut child = cmd.stdout(Stdio::piped()).spawn()?;

        let mut output_lines = BufReader::new(child.stdout.take().unwrap()).lines();
        if let Some(pb) = progress {
            while let Some(line) = output_lines.next_line().await? {
                pb.set_message(format!("pip install: {line}"));
            }
        }

        if child.wait().await?.success() {
            Ok(())
        } else {
            Err(error::system(&format!("pip install failed"), ""))
        }
    }

    pub async fn build_package(
        &self,
        input: impl AsRef<Path>,
        output: impl AsRef<Path>,
        progress: Option<&ProgressBar>,
    ) -> Result<()> {
        if !self.is_module_installed("build").await? {
            self.pip_install(["build"], &Default::default(), progress)
                .await?;
        }
        let mut child = self
            .python_cmd()
            .arg("-m")
            .arg("build")
            .arg("--sdist")
            .arg("--outdir")
            .arg(output.as_ref().as_os_str())
            .arg(input.as_ref().as_os_str())
            .stdout(Stdio::piped())
            .spawn()?;

        let mut output_lines = BufReader::new(child.stdout.take().unwrap()).lines();
        if let Some(pb) = progress {
            while let Some(line) = output_lines.next_line().await? {
                pb.set_message(format!("building package: {line}"));
            }
        }

        if child.wait().await?.success() {
            Ok(())
        } else {
            Err(error::user(
                &format!("Could not build package {}", output.as_ref().display()),
                "",
            ))
        }
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
        })?
        .await?
    };
}
pub(crate) use async_python_run;

pub fn async_generator(generator: Py<PyAny>) -> PyResult<impl Stream<Item = PyResult<Py<PyAny>>>> {
    let generator = Python::with_gil(move |py| {
        generator
            .as_ref(py)
            .call_method0(pyo3::intern!(py, "__aiter__"))?;
        PyResult::Ok(generator)
    })?;
    Ok(futures::stream::unfold(
        generator,
        move |generator| async move {
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
        },
    ))
}

pub fn deepcopy(obj: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| {
        let copy = py.import("copy")?.getattr("deepcopy")?;
        Ok(copy.call1((obj,))?.into_py(py))
    })
}

type AsyncIteratorStream = futures::stream::BoxStream<'static, PyResult<Py<PyAny>>>;

#[pyclass]
pub struct AsyncIterator {
    stream: std::sync::Arc<std::sync::Mutex<Option<AsyncIteratorStream>>>,
}

impl AsyncIterator {
    pub fn new(stream: impl Stream<Item = PyResult<Py<PyAny>>> + Send + Sync + 'static) -> Self {
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
                    Some(Ok(value)) => Ok(value),
                    Some(Err(err)) => Err(err),
                    None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
                }
            },
        )?;
        Ok(IterANextOutput::Yield(result))
    }
}
