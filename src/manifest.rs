use aqora_config::{pyproject_toml, Version};

lazy_static::lazy_static! {
    pub static ref MANIFEST: pyproject_toml::PyProjectToml = toml::from_str(include_str!("../pyproject.toml")).unwrap();
    pub static ref CARGO_PKG_VERSION: Version = env!("CARGO_PKG_VERSION").parse().unwrap();
}

pub fn manifest_version() -> &'static Version {
    MANIFEST
        .project
        .as_ref()
        .and_then(|project| project.version.as_ref())
        .unwrap_or(&CARGO_PKG_VERSION)
}

pub fn manifest_name() -> &'static str {
    MANIFEST
        .project
        .as_ref()
        .map(|project| project.name.as_str())
        .unwrap()
}

pub fn parse_aqora_version(version_output: &str) -> Option<Version> {
    version_output
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
}
