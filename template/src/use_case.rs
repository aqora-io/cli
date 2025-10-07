use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use serde::Serialize;

use crate::registry::REGISTRY;
use crate::utils::{assert_no_control_chars, assert_semver, assert_slug};

const DEFAULT_PYTHON_VERSION: &str = "3.8";
const DEFAULT_CLI_VERSION_STR: &str = env!("CARGO_PKG_VERSION");

#[derive(Builder, Serialize, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct UseCaseTemplate {
    #[builder(setter(into), default = "DEFAULT_PYTHON_VERSION.to_string()")]
    python_version: String,
    #[builder(setter(into), default = "DEFAULT_CLI_VERSION_STR.to_string()")]
    cli_version: String,
    #[builder(setter(into))]
    competition: String,
    #[builder(setter(into))]
    title: String,
}

impl UseCaseTemplate {
    pub fn builder() -> UseCaseTemplateBuilder {
        UseCaseTemplateBuilder::default()
    }

    pub fn render(&self, out: impl AsRef<Path>) -> Result<(), RenderError> {
        REGISTRY.render_all("use_case", self, out)
    }
}

impl UseCaseTemplateBuilder {
    fn validate(&self) -> Result<(), String> {
        self.python_version
            .as_deref()
            .map(assert_semver)
            .transpose()?;
        self.cli_version.as_deref().map(assert_semver).transpose()?;
        assert_no_control_chars(self.title.as_ref().ok_or("Title is required")?)?;
        assert_slug(self.competition.as_ref().ok_or("Competition is required")?)?;
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
    use tempfile::TempDir;

    #[test]
    fn test_use_case() {
        let tmp = TempDir::new().unwrap();
        UseCaseTemplate::builder()
            .title("This is a test")
            .competition("this-is-a-test")
            .render(tmp.path())
            .unwrap();
        toml::from_str::<toml::Value>(
            std::fs::read_to_string(tmp.path().join("pyproject.toml"))
                .unwrap()
                .as_str(),
        )
        .unwrap();
        std::fs::read_to_string(tmp.path().join(".gitignore")).unwrap();
    }
}
