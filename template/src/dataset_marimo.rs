use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use serde::Serialize;

use crate::registry::REGISTRY;
use crate::utils::{
    assert_python_minor, assert_semver, assert_slug, assert_username, OptionExt,
    DEFAULT_PYTHON_VERSION,
};

const DEFAULT_MARIMO_VERSION: &str = "0.25.0";
const DEFAULT_CLI_VERSION_STR: &str = env!("CARGO_PKG_VERSION");

#[derive(Builder, Serialize, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct DatasetMarimoTemplate {
    #[builder(setter(into), default = "DEFAULT_PYTHON_VERSION.to_string()")]
    python_version: String,
    #[builder(setter(into), default = "DEFAULT_CLI_VERSION_STR.to_string()")]
    cli_version: String,
    #[builder(setter(into), default = "DEFAULT_MARIMO_VERSION.to_string()")]
    marimo_version: String,
    #[builder(setter(into))]
    name: String,
    #[builder(setter(into, strip_option), default)]
    owner: Option<String>,
    #[builder(setter(into, strip_option), default)]
    local_slug: Option<String>,
    #[builder(setter(into, strip_option), default)]
    version: Option<String>,
    #[builder(setter(into, strip_option), default)]
    raw_init: Option<String>,
}

impl DatasetMarimoTemplate {
    pub fn builder() -> DatasetMarimoTemplateBuilder {
        DatasetMarimoTemplateBuilder::default()
    }

    pub fn render(&self, out: impl AsRef<Path>) -> Result<(), RenderError> {
        REGISTRY.render_all("dataset_marimo", self, out)
    }
}

impl DatasetMarimoTemplateBuilder {
    pub fn validate(&self) -> Result<(), String> {
        self.python_version
            .as_deref()
            .map(assert_python_minor)
            .transpose()?;
        self.cli_version.as_deref().map(assert_semver).transpose()?;
        self.marimo_version
            .as_deref()
            .map(assert_semver)
            .transpose()?;
        assert_slug(self.name.as_ref().ok_or("Name is required")?)?;
        if self.raw_init.flat_ref().is_none() {
            assert_username(self.owner.flat_ref().ok_or("Owner is required")?)?;
            assert_slug(self.local_slug.flat_ref().ok_or("Local slug is required")?)?;
            assert_semver(self.version.flat_ref().ok_or("Version is required")?)?;
        }
        Ok(())
    }

    pub fn render(&self, out: impl AsRef<Path>) -> Result<(), RenderError> {
        self.build()
            .map_err(|e| RenderErrorReason::Other(e.to_string()))?
            .render(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::extract_pep723_toml;

    /// The `raw_init` variant: no dataset backing it, the caller supplies the
    /// loading code directly (e.g. `aqora new dataset-marimo --raw-init ...`).
    fn render_raw_init() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("ws");
        DatasetMarimoTemplate::builder()
            .name("my-dataset")
            .raw_init("data = None")
            .render(&out)
            .expect("render");
        (dir, out)
    }

    /// The dataset-slug variant: loads a published dataset by owner/slug/version.
    fn render_dataset_slug() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("ws");
        DatasetMarimoTemplate::builder()
            .name("my-dataset")
            .owner("someone")
            .local_slug("a-dataset")
            .version("0.1.0")
            .render(&out)
            .expect("render");
        (dir, out)
    }

    /// Unlike the hosted-only workspace template, this scaffold is dual-use:
    /// `aqora new dataset-marimo` lands on a user's own machine, where git
    /// hygiene needs `.gitignore` — and the hosted indexer honours it just the
    /// same. `.secrets` documents the hosted secret handling either way.
    #[test]
    fn the_scaffold_keeps_gitignore_and_gains_secrets() {
        let (_dir, out) = render_dataset_slug();
        assert!(out.join(".gitignore").is_file());
        let secrets = std::fs::read_to_string(out.join(".secrets")).unwrap();
        assert!(
            secrets
                .lines()
                .all(|line| line.trim().is_empty() || line.starts_with('#')),
            "{secrets}"
        );
    }

    /// The header is what makes the notebook runnable on its own via
    /// `uvx marimo edit --sandbox`, and what kubimo's pixi environment is
    /// built from — for both variants of the scaffold.
    #[test]
    fn the_header_declares_what_mo_sql_and_aqora_pyarrow_need() {
        for (_dir, out) in [render_raw_init(), render_dataset_slug()] {
            let readme = std::fs::read_to_string(out.join("readme.py")).unwrap();
            let header = extract_pep723_toml(&readme);
            let deps: Vec<&str> = header["dependencies"]
                .as_array()
                .expect("dependencies array")
                .iter()
                .map(|dep| dep.as_str().expect("dependency is a string"))
                .collect();
            let expected_aqora = format!("aqora[pyarrow]>={DEFAULT_CLI_VERSION_STR}");
            assert!(deps.contains(&"marimo"), "{deps:?}");
            assert!(deps.contains(&expected_aqora.as_str()), "{deps:?}");
            assert!(deps.contains(&"duckdb"), "{deps:?}");
            assert!(deps.contains(&"polars"), "{deps:?}");
            assert!(deps.contains(&"sqlglot"), "{deps:?}");
        }
    }

    #[test]
    fn no_rendered_file_contains_the_venv_table() {
        for (_dir, out) in [render_raw_init(), render_dataset_slug()] {
            for entry in std::fs::read_dir(&out).unwrap() {
                let path = entry.unwrap().path();
                let contents = std::fs::read_to_string(&path).unwrap();
                assert!(
                    !contents.contains("[tool.marimo.venv]"),
                    "{}",
                    path.display()
                );
            }
        }
    }
}
