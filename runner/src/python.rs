use crate::ipython::override_get_ipython;
use aqora_config::{PackageName, PathStr};
#[cfg(feature = "clap")]
use clap::{builder::PossibleValue, ValueEnum};
use futures::prelude::*;
use pyo3::{intern, prelude::*, types::PyType};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::process::Command;

lazy_static::lazy_static! {
    static ref PYTHON_VERSION: String = Python::with_gil(|py| {
        let version = py.version_info();
        format!("{}.{}", version.major, version.minor)
    });
}

pub type PyBoundObject<'py> = Bound<'py, PyAny>;

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColorChoice {
    #[default]
    Auto,
    Always,
    Never,
}

impl ColorChoice {
    fn apply(&self, cmd: &mut Command) {
        cmd.arg("--color").arg(self);
    }
}

impl AsRef<OsStr> for ColorChoice {
    fn as_ref(&self) -> &OsStr {
        match self {
            Self::Auto => OsStr::new("auto"),
            Self::Always => OsStr::new("always"),
            Self::Never => OsStr::new("never"),
        }
    }
}

#[cfg(feature = "clap")]
impl ValueEnum for ColorChoice {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Auto, Self::Always, Self::Never]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(match self {
            Self::Auto => PossibleValue::new("auto"),
            Self::Always => PossibleValue::new("always"),
            Self::Never => PossibleValue::new("never"),
        })
    }
}

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkMode {
    #[default]
    Copy,
    Clone,
    Hardlink,
    Symlink,
}

impl LinkMode {
    fn apply(&self, cmd: &mut Command) {
        cmd.arg("--link-mode").arg(self);
    }
}

impl AsRef<OsStr> for LinkMode {
    fn as_ref(&self) -> &OsStr {
        match self {
            Self::Copy => OsStr::new("copy"),
            Self::Clone => OsStr::new("clone"),
            Self::Hardlink => OsStr::new("hardlink"),
            Self::Symlink => OsStr::new("symlink"),
        }
    }
}

#[cfg(feature = "clap")]
impl ValueEnum for LinkMode {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Copy, Self::Clone, Self::Hardlink]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(match self {
            Self::Copy => PossibleValue::new("copy"),
            Self::Clone => PossibleValue::new("clone"),
            Self::Hardlink => PossibleValue::new("hardlink"),
            Self::Symlink => PossibleValue::new("symlink"),
        })
    }
}

#[derive(Default)]
pub struct PipOptions {
    pub upgrade: bool,
    pub no_deps: bool,
    pub color: ColorChoice,
    pub link_mode: LinkMode,
}

pub enum PipPackage {
    Pypi(OsString),
    Tar(OsString, OsString),
    Editable(PathBuf),
}

impl fmt::Display for PipPackage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pypi(name) => write!(f, "{}", name.to_string_lossy()),
            Self::Tar(name, source) => write!(
                f,
                "{} @ {}",
                name.to_string_lossy(),
                source.to_string_lossy()
            ),
            Self::Editable(path) => write!(f, "--editable {}", path.display()),
        }
    }
}

impl PipPackage {
    pub fn pypi(name: impl Into<OsString>) -> Self {
        Self::Pypi(name.into())
    }

    pub fn tar(name: impl Into<OsString>, version: impl Into<OsString>) -> Self {
        Self::Tar(name.into(), version.into())
    }

    pub fn editable(path: impl Into<PathBuf>) -> Self {
        Self::Editable(path.into())
    }

    pub fn name(&self) -> String {
        match self {
            Self::Pypi(name) => name.as_os_str(),
            Self::Tar(name, _) => name.as_os_str(),
            Self::Editable(path) => path.as_os_str(),
        }
        .to_string_lossy()
        .to_string()
    }

    fn apply(&self, cmd: &mut Command) {
        match self {
            Self::Pypi(name) => {
                cmd.arg(name);
            }
            Self::Tar(name, source) => {
                let mut arg = OsString::from(name);
                arg.push(" @ ");
                arg.push(source);
                cmd.arg(arg);
            }
            Self::Editable(path) => {
                cmd.arg("--editable").arg(path);
            }
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub const BIN_PATH: &str = "bin";
#[cfg(target_os = "windows")]
pub const BIN_PATH: &str = "Scripts";

#[derive(Debug, Clone)]
pub struct PyEnv {
    venv_path: PathBuf,
    cache_path: Option<PathBuf>,
}

#[derive(Error, Debug)]
pub enum EnvError {
    #[error("Error processing {0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("{}", format_err(_0))]
    Python(#[from] PyErr),
    #[error("Failed to setup virtualenv: {0} ({1})")]
    VenvFailed(String, String),
}

#[derive(Clone, Debug, Default)]
pub struct PyEnvOptions {
    pub cache_path: Option<PathBuf>,
    pub python: Option<String>,
    pub color: ColorChoice,
    pub link_mode: LinkMode,
}

impl PyEnv {
    pub async fn init(
        uv_path: impl AsRef<Path>,
        venv_path: impl AsRef<Path>,
        options: PyEnvOptions,
    ) -> Result<Self, EnvError> {
        let cache_path = if let Some(cache_path) = options.cache_path.as_ref() {
            tokio::fs::create_dir_all(&cache_path)
                .await
                .map_err(|err| EnvError::Io(cache_path.to_path_buf(), err))?;
            Some(
                dunce::canonicalize(cache_path)
                    .map_err(|err| EnvError::Io(cache_path.to_path_buf(), err))?,
            )
        } else {
            None
        };
        let venv_path = venv_path.as_ref();
        Self::ensure_venv(
            &uv_path,
            &venv_path,
            PyEnvOptions {
                cache_path: cache_path.clone(),
                ..options
            },
        )
        .await?;
        let venv_path = dunce::canonicalize(venv_path)
            .map_err(|err| EnvError::Io(venv_path.to_path_buf(), err))?;
        Python::with_gil(|py| {
            let sys = py.import(intern!(py, "sys"))?;
            let prefix = sys.getattr(intern!(py, "prefix"))?.extract::<PathBuf>()?;
            if prefix == venv_path {
                return Ok(());
            }
            let runpy = py.import(intern!(py, "runpy"))?;
            runpy
                .getattr(intern!(py, "run_path"))?
                .call1((venv_path.join(BIN_PATH).join("activate_this.py"),))?;
            override_get_ipython(py)?;
            PyResult::Ok(())
        })?;
        Ok(Self {
            venv_path,
            cache_path,
        })
    }

    async fn ensure_venv(
        uv_path: impl AsRef<Path>,
        venv_path: impl AsRef<Path>,
        options: PyEnvOptions,
    ) -> Result<(), EnvError> {
        let uv_path = uv_path.as_ref();
        let path = venv_path.as_ref();
        if !path.join("pyvenv.cfg").exists() {
            let mut cmd = Command::new(uv_path);
            cmd.arg("venv")
                .arg("--python")
                .arg(
                    options
                        .python
                        .as_ref()
                        .map(|p| p.as_ref())
                        .unwrap_or_else(|| PYTHON_VERSION.as_str()),
                )
                .arg(venv_path.as_ref());
            if let Some(cache_path) = options.cache_path.as_ref() {
                cmd.arg("--cache-dir").arg(cache_path);
            }
            options.color.apply(&mut cmd);
            options.link_mode.apply(&mut cmd);
            let output = cmd
                .output()
                .await
                .map_err(|err| EnvError::Io(uv_path.to_path_buf(), err))?;
            if !output.status.success() {
                return Err(EnvError::VenvFailed(
                    String::from_utf8_lossy(&output.stderr).to_string(),
                    format!("{:?}", cmd.as_std()),
                ));
            }
        }
        let mut cmd = Command::new(uv_path);
        cmd.env("VIRTUAL_ENV", path)
            .arg("pip")
            .arg("install")
            .arg("aqora-cli[venv]");
        if let Some(cache_path) = options.cache_path.as_ref() {
            cmd.arg("--cache-dir").arg(cache_path);
        }
        options.color.apply(&mut cmd);
        options.link_mode.apply(&mut cmd);
        let output = cmd
            .output()
            .await
            .map_err(|err| EnvError::Io(uv_path.to_path_buf(), err))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(EnvError::VenvFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
                format!("{:?}", cmd.as_std()),
            ))
        }
    }

    pub fn venv_path(&self) -> &Path {
        &self.venv_path
    }

    pub fn bin_path(&self) -> PathBuf {
        self.venv_path.join(BIN_PATH)
    }

    pub fn cache_path(&self) -> Option<&Path> {
        self.cache_path.as_deref()
    }

    pub fn python_path(&self) -> PathBuf {
        self.bin_path().join("python")
    }

    pub fn uv_path(&self) -> PathBuf {
        self.bin_path().join("uv")
    }

    pub fn activate_path(&self) -> PathBuf {
        self.bin_path().join("activate")
    }

    pub fn python_cmd(&self) -> Command {
        let mut cmd = Command::new(self.python_path().as_os_str());
        cmd.env("VIRTUAL_ENV", self.venv_path.as_os_str());
        cmd
    }

    pub fn uv_cmd(&self) -> Command {
        let mut cmd = Command::new(self.uv_path().as_os_str());
        cmd.env("VIRTUAL_ENV", self.venv_path.as_os_str());
        if let Some(cache_path) = self.cache_path.as_ref() {
            cmd.arg("--cache-dir").arg(cache_path.as_os_str());
        }
        cmd
    }

    pub fn pip_install(
        &self,
        modules: impl IntoIterator<Item = PipPackage>,
        opts: &PipOptions,
    ) -> Command {
        let mut cmd = self.uv_cmd();
        cmd.arg("pip").arg("install");
        if opts.upgrade {
            cmd.arg("--upgrade");
        }
        if opts.no_deps {
            cmd.arg("--no-deps");
        }
        opts.color.apply(&mut cmd);
        opts.link_mode.apply(&mut cmd);
        for module in modules {
            module.apply(&mut cmd);
        }
        cmd
    }

    pub fn pip_uninstall(
        &self,
        modules: impl IntoIterator<Item = PackageName>,
        opts: &PipOptions,
    ) -> Command {
        let mut cmd = self.uv_cmd();
        cmd.arg("pip").arg("uninstall");
        opts.color.apply(&mut cmd);
        for module in modules {
            cmd.arg(module.to_string());
        }
        cmd
    }

    pub fn build_package(&self, input: impl AsRef<Path>, output: impl AsRef<Path>) -> Command {
        let mut cmd = self.python_cmd();
        cmd.arg("-m")
            .arg("build")
            .arg("--sdist")
            .arg("--installer")
            .arg("uv")
            .arg("--outdir")
            .arg(output.as_ref().as_os_str())
            .arg(input.as_ref().as_os_str());
        cmd
    }

    pub fn import_path<'py>(
        &self,
        py: Python<'py>,
        path: &PathStr,
    ) -> PyResult<PyBoundObject<'py>> {
        let module_name = path.module().to_string();
        let module = PyModule::import(py, &module_name)?;
        module.getattr(path.name())
    }

    pub fn find_spec_search_locations(&self, py: Python, path: &PathStr) -> PyResult<Vec<PathBuf>> {
        let importlib = py.import(intern!(py, "importlib.util"))?;
        let spec = importlib
            .getattr(intern!(py, "find_spec"))?
            .call1((&path.module().to_string(),))?;
        if spec.is_none() {
            return Ok(Vec::new());
        }
        let locations = spec.getattr("submodule_search_locations")?;
        if locations.is_none() {
            return Ok(Vec::new());
        }
        locations
            .try_iter()?
            .map(|path| path.and_then(|p| p.extract::<PathBuf>()))
            .collect()
    }
}

#[macro_export]
macro_rules! async_python_run {
    ($($closure:tt)*) => {
        Python::with_gil(|py| {
            let closure = $($closure)*;
            let awaitable = match closure(py) {
                Ok(awaitable) => awaitable,
                Err(err) => return Err(err),
            };
            pyo3_async_runtimes::into_future_with_locals(
                &pyo3_async_runtimes::tokio::get_current_locals(py)?,
                awaitable
            )
        })
    };
}
pub(crate) use async_python_run;

pub fn async_generator(generator: PyObject) -> PyResult<impl Stream<Item = PyResult<PyObject>>> {
    let generator = Python::with_gil(move |py| {
        generator
            .bind_borrowed(py)
            .call_method0(pyo3::intern!(py, "__aiter__"))?;
        PyResult::Ok(generator)
    })?;

    Ok(
        futures::stream::unfold(generator, move |generator| async move {
            let result = match Python::with_gil(|py| {
                pyo3_async_runtimes::into_future_with_locals(
                    &pyo3_async_runtimes::tokio::get_current_locals(py)?,
                    generator
                        .bind_borrowed(py)
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
                        .is(&PyType::new::<pyo3::exceptions::PyStopAsyncIteration>(py))
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

pub fn deepcopy<'py>(obj: &PyBoundObject<'py>) -> PyResult<PyBoundObject<'py>> {
    let py = obj.py();
    py.import(intern!(py, "copy"))?
        .getattr(intern!(py, "deepcopy"))?
        .call1((obj,))
}

pub mod serde_pickle {
    use pyo3::prelude::*;
    use std::borrow::Cow;

    pub(crate) struct BytesVisitor;

    impl<'de> serde::de::Visitor<'de> for BytesVisitor {
        type Value = Cow<'de, [u8]>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bytes")
        }
        fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E> {
            Ok(Cow::Borrowed(v))
        }
        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Cow::Owned(v.to_vec()))
        }
        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
            Ok(Cow::Owned(v))
        }
    }

    pub fn serialize<S, T>(value: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
        T: for<'py> IntoPyObject<'py>,
    {
        Python::with_gil(|py| {
            let out = py
                .import("pickle")
                .map_err(serde::ser::Error::custom)?
                .getattr("dumps")
                .map_err(serde::ser::Error::custom)?
                .call1((value,))
                .map_err(serde::ser::Error::custom)?;
            let bytes = out.extract().map_err(serde::ser::Error::custom)?;
            serializer.serialize_bytes(bytes)
        })
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::de::Deserializer<'de>,
        T: for<'a> FromPyObject<'a>,
    {
        let bytes = deserializer.deserialize_any(BytesVisitor)?;
        Python::with_gil(|py| {
            let out = py.import("pickle")?.getattr("loads")?.call1((bytes,))?;
            FromPyObject::extract_bound(&out)
        })
        .map_err(serde::de::Error::custom)
    }

    pub fn deserialize_pyerr<'de, D>(deserializer: D) -> Result<PyErr, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let bytes = deserializer.deserialize_any(BytesVisitor)?;
        Python::with_gil(|py| {
            let out = py
                .import("pickle")?
                .getattr("loads")?
                .call1((bytes.as_ref(),))?;
            PyResult::Ok(PyErr::from_value(out))
        })
        .map_err(serde::de::Error::custom)
    }
}

pub mod serde_pickle_opt {
    use super::serde_pickle;
    use pyo3::prelude::*;
    use std::borrow::Cow;

    struct MaybeBytesVisitor;

    impl<'de> serde::de::Visitor<'de> for MaybeBytesVisitor {
        type Value = Option<Cow<'de, [u8]>>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("maybe bytes")
        }
        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::de::Deserializer<'de>,
        {
            let bytes = deserializer.deserialize_any(serde_pickle::BytesVisitor)?;
            Ok(Some(bytes))
        }
    }

    // NOTE: We can only use unbound types here because of recursion on the trait implementation on
    // the borrowed &'a T and we don't want to use clone unnecessarily
    pub fn serialize<'a, S, T>(value: &'a Option<Py<T>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
        &'a Py<T>: for<'py> IntoPyObject<'py>,
    {
        match value {
            Some(value) => serde_pickle::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: serde::de::Deserializer<'de>,
        T: for<'a> FromPyObject<'a>,
    {
        let bytes = deserializer.deserialize_option(MaybeBytesVisitor)?;
        if let Some(bytes) = bytes {
            Python::with_gil(|py| {
                let out = py
                    .import("pickle")?
                    .getattr("loads")?
                    .call1((bytes.as_ref(),))?;
                FromPyObject::extract_bound(&out)
            })
            .map_err(serde::de::Error::custom)
            .map(Some)
        } else {
            Ok(None)
        }
    }
}

pub fn format_err(pyerr: &PyErr) -> String {
    Python::with_gil(|py| {
        let formatter = py.import("traceback")?.getattr("format_exc")?;
        pyerr.clone_ref(py).restore(py);
        formatter.call1((1,))?.extract()
    })
    .unwrap_or_else(|err| err.to_string())
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
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<PyBoundObject<'py>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py_with_locals(
            py,
            pyo3_async_runtimes::tokio::get_current_locals(py)?,
            async move {
                match stream.lock().await.next().await {
                    Some(value) => Ok(value),
                    None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
                }
            },
        )
    }
}
