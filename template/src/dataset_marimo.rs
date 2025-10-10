use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use serde::Serialize;

use crate::registry::REGISTRY;
use crate::utils::{assert_semver, assert_slug, assert_username};

const DEFAULT_PYTHON_VERSION: &str = "3.10";
const DEFAULT_MARIMO_VERSION: &str = "0.16.4";
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
    owner: String,
    #[builder(setter(into))]
    local_slug: String,
    #[builder(setter(into))]
    version: String,
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
        assert_username(self.owner.as_ref().ok_or("Owner is required")?)?;
        assert_slug(self.local_slug.as_ref().ok_or("Local slug is required")?)?;
        assert_semver(self.version.as_ref().ok_or("Version is required")?)?;
        Ok(())
    }

    pub fn render(&self, out: impl AsRef<Path>) -> Result<(), RenderError> {
        self.build()
            .map_err(|e| RenderErrorReason::Other(e.to_string()))?
            .render(out)
    }
}
