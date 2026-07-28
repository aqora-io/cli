use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use serde::Serialize;

use crate::registry::REGISTRY;
use crate::utils::{assert_semver, assert_slug, assert_username, OptionExt};

const DEFAULT_PYTHON_VERSION: &str = "3.10";
const DEFAULT_MARIMO_VERSION: &str = "0.23.4";
const DEFAULT_CLI_VERSION_STR: &str = env!("CARGO_PKG_VERSION");
/// Where the kubimo marimo image keeps a workspace's venv. Only used when
/// `hosted` is set.
const DEFAULT_VENV_PATH: &str = "/home/me/venv";

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
    /// Scaffold for an image that already provides marimo and aqora in its
    /// system interpreter, rather than for a user's own machine.
    ///
    /// Off by default: `aqora new dataset-marimo` runs locally, where nothing
    /// else installs marimo, so the generated project must declare it. A hosted
    /// workspace must not — a venv copy shadows the image's build and leaves
    /// kernels on a different marimo from the server.
    #[builder(default)]
    hosted: bool,
    /// Virtualenv path recorded under `[tool.marimo.venv]`, hosted only.
    #[builder(setter(into), default = "DEFAULT_VENV_PATH.to_string()")]
    venv_path: String,
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
            .map(assert_semver)
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

    fn render(hosted: bool) -> String {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("ws");
        DatasetMarimoTemplate::builder()
            .name("my-dataset")
            .owner("someone")
            .local_slug("a-dataset")
            .version("0.1.0")
            .hosted(hosted)
            .render(&out)
            .expect("render");
        std::fs::read_to_string(out.join("pyproject.toml")).unwrap()
    }

    /// `aqora new dataset-marimo` runs on a user's own machine, where nothing
    /// else installs marimo. The generated project has to declare it, or the
    /// workspace has no marimo at all.
    #[test]
    fn a_local_scaffold_declares_marimo_and_aqora() {
        let pyproject = render(false);
        assert!(pyproject.contains("marimo[recommended,lsp]"), "{pyproject}");
        assert!(pyproject.contains("aqora[pyarrow]"), "{pyproject}");
        // No venv table: the path is a kubimo image detail and would be wrong
        // anywhere else.
        assert!(!pyproject.contains("[tool.marimo.venv]"), "{pyproject}");
    }

    /// A hosted workspace inherits both from the image's system interpreter.
    /// Declaring them installs shadowing copies into the venv — for marimo a
    /// ~920MB duplicate that also splits the kernel from the server.
    #[test]
    fn a_hosted_scaffold_declares_neither() {
        let pyproject = render(true);
        let deps = pyproject
            .split("dependencies = [")
            .nth(1)
            .and_then(|rest| rest.split(']').next())
            .expect("dependencies array");
        assert!(!deps.contains("marimo"), "{deps}");
        assert!(!deps.contains("aqora"), "{deps}");
        assert!(pyproject.contains("[tool.marimo.venv]"), "{pyproject}");
        assert!(pyproject.contains("writable = false"), "{pyproject}");
        assert!(pyproject.contains("/home/me/venv"), "{pyproject}");
    }

    /// Both variants must still be valid TOML — the `{{#if}}` blocks are easy
    /// to get subtly wrong in a way that only shows up when uv parses it.
    #[test]
    fn both_variants_parse_as_toml() {
        for hosted in [false, true] {
            let pyproject = render(hosted);
            toml::from_str::<toml::Value>(&pyproject)
                .unwrap_or_else(|err| panic!("hosted={hosted}: {err}\n{pyproject}"));
        }
    }
}
