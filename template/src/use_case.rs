use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use regex::Regex;
use serde::Serialize;

use crate::registry::REGISTRY;

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
        lazy_static::lazy_static! {
            static ref SEMVER_REGEX: Regex = Regex::new(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$").unwrap();
            static ref SLUG_REGEX: Regex = Regex::new(r"^[-a-zA-Z0-9_]*$").unwrap();
        }

        if let Some(python_version) = self.python_version.as_ref() {
            if !SEMVER_REGEX.is_match(python_version) {
                return Err(format!("Invalid Python version: {}", python_version));
            }
        }
        if let Some(cli_version) = self.cli_version.as_ref() {
            if !SEMVER_REGEX.is_match(cli_version) {
                return Err(format!("Invalid CLI version: {}", cli_version));
            }
        }
        let title = self.title.as_ref().ok_or("Title is required")?;
        if title.contains(|c: char| c.is_control()) {
            return Err("Title must not contain control characters".to_string());
        }
        let competition = self.competition.as_ref().ok_or("Competition is required")?;
        if !SLUG_REGEX.is_match(competition) {
            return Err("Competition must be a valid slug".to_string());
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
