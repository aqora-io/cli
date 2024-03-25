use crate::manifest::manifest_version;
use pyo3::Python;

lazy_static::lazy_static! {
    static ref VERSION: String = format!(
        "{}\nPython {}",
        manifest_version(),
        Python::with_gil(|py| py.version().to_string())
    );
}

pub fn version() -> clap::builder::Str {
    VERSION.as_str().into()
}
