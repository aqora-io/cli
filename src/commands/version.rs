use crate::manifest::manifest_version;
use pyo3::Python;

lazy_static::lazy_static! {
    static ref PYTHON_VERSION: String = Python::with_gil(|py| py.version().to_string());
    static ref VERSION: String = format!("{}\nPython {}", manifest_version(), *PYTHON_VERSION);
}

pub fn version() -> clap::builder::Str {
    VERSION.as_str().into()
}

pub fn python_version() -> &'static str {
    PYTHON_VERSION.as_str()
}
