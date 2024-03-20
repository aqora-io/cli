use pyo3::Python;

const AQORA_PYPROJECT: &str = include_str!("../../pyproject.toml");

lazy_static::lazy_static! {
    static ref VERSION: String = format!(
        "{}\nPython {}",
        toml::from_str::<toml::Value>(AQORA_PYPROJECT)
            .ok()
            .and_then(|toml| {
                toml.get("project")
                    .and_then(|project| project.get("version"))
                    .and_then(|version| version.as_str())
                    .map(|version| version.to_string())
            })
            .unwrap_or_else(|| clap::crate_version!().to_string()),
        Python::with_gil(|py| py.version().to_string())
    );
}

pub fn version() -> clap::builder::Str {
    VERSION.as_str().into()
}
