use aqora_config::{pyproject_toml, Version};

lazy_static::lazy_static! {
    pub static ref MANIFEST: pyproject_toml::PyProjectToml = toml::from_str(include_str!("../pyproject.toml")).unwrap();
}

pub fn manifest_version() -> &'static Version {
    MANIFEST
        .project
        .as_ref()
        .and_then(|project| project.version.as_ref())
        .unwrap()
}

pub fn manifest_name() -> &'static str {
    MANIFEST
        .project
        .as_ref()
        .map(|project| project.name.as_str())
        .unwrap()
}
