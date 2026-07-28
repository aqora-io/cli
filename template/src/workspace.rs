use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use serde::Serialize;

use crate::registry::REGISTRY;
use crate::utils::{assert_python_raw_string_safe, assert_semver, assert_slug};

const DEFAULT_PYTHON_VERSION: &str = "3.10";
const DEFAULT_MARIMO_VERSION: &str = "0.23.4";
const DEFAULT_VERSION: &str = "0.0.0";

/// The starter contents of a plain marimo workspace.
///
/// The dataset variant is [`crate::DatasetMarimoTemplate`]; this one has no
/// dataset to wire up, so its notebook is just a welcome page. Both exist so
/// that the two kinds of workspace are scaffolded the same way, from templates
/// rather than from a shell heredoc built at the call site.
#[derive(Builder, Serialize, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct WorkspaceTemplate {
    #[builder(setter(into), default = "DEFAULT_PYTHON_VERSION.to_string()")]
    python_version: String,
    #[builder(setter(into), default = "DEFAULT_MARIMO_VERSION.to_string()")]
    marimo_version: String,
    /// Display name, shown in the notebook's heading.
    #[builder(setter(into))]
    name: String,
    /// Slug-safe identifier, used as the Python project name.
    #[builder(setter(into))]
    slug: String,
    /// Workspace version, shown alongside the name.
    #[builder(setter(into), default = "DEFAULT_VERSION.to_string()")]
    version: String,
}

impl WorkspaceTemplate {
    pub fn builder() -> WorkspaceTemplateBuilder {
        WorkspaceTemplateBuilder::default()
    }

    pub fn render(&self, out: impl AsRef<Path>) -> Result<(), RenderError> {
        REGISTRY.render_all("workspace", self, out)
    }
}

impl WorkspaceTemplateBuilder {
    pub fn validate(&self) -> Result<(), String> {
        self.python_version
            .as_deref()
            .map(assert_semver)
            .transpose()?;
        self.marimo_version
            .as_deref()
            .map(assert_semver)
            .transpose()?;
        self.version.as_deref().map(assert_semver).transpose()?;
        // The slug becomes the `[project] name` in pyproject.toml, so it is a slug.
        assert_slug(self.slug.as_ref().ok_or("Slug is required")?)?;
        // The display name stays free text, but "markdown" understates where it lands:
        // it is markdown *inside a Python raw string literal* in readme.py, rendered
        // without escaping. It has to be safe for the literal as well as legible.
        let name = self.name.as_ref().ok_or("Name is required")?;
        if name.trim().is_empty() {
            return Err("Name must not be blank".to_string());
        }
        assert_python_raw_string_safe(name)?;
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

    fn rendered() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("workspace");
        WorkspaceTemplate::builder()
            .name("My Workspace")
            .slug("my-workspace")
            .render(&out)
            .expect("render");
        (dir, out)
    }

    #[test]
    fn renders_the_three_starter_files() {
        let (_dir, out) = rendered();
        for file in ["readme.py", "pyproject.toml", ".gitignore"] {
            assert!(out.join(file).is_file(), "missing {file}");
        }
    }

    /// `readme.py` is not merely cosmetic: the platform picks it as a
    /// workspace's default overview notebook by exact filename.
    #[test]
    fn the_notebook_carries_the_display_name_and_version() {
        let (_dir, out) = rendered();
        let readme = std::fs::read_to_string(out.join("readme.py")).unwrap();
        assert!(readme.contains("# My Workspace v0.0.0"), "{readme}");
        assert!(readme.contains("import marimo"));
    }

    /// Declaring marimo here would make `uv sync` install a second copy into
    /// the venv, shadowing the image's system build — a ~920MB duplicate that
    /// also leaves kernels on a different marimo from the server.
    #[test]
    fn the_pyproject_declares_neither_marimo_nor_aqora() {
        let (_dir, out) = rendered();
        let pyproject = std::fs::read_to_string(out.join("pyproject.toml")).unwrap();
        let deps = pyproject
            .split("dependencies = [")
            .nth(1)
            .and_then(|rest| rest.split(']').next())
            .expect("dependencies array");
        assert!(!deps.contains("marimo"), "{deps}");
        assert!(!deps.contains("aqora"), "{deps}");
        // And the venv table must be present, so the runner does not have to
        // append it at startup — which would rewrite a tracked file on every
        // boot and re-upload it.
        assert!(pyproject.contains("[tool.marimo.venv]"), "{pyproject}");
        assert!(pyproject.contains("writable = false"), "{pyproject}");
    }

    #[test]
    fn the_project_name_is_the_slug_not_the_display_name() {
        let (_dir, out) = rendered();
        let pyproject = std::fs::read_to_string(out.join("pyproject.toml")).unwrap();
        assert!(
            pyproject.contains(r#"name = "my-workspace""#),
            "{pyproject}"
        );
    }

    #[test]
    fn a_non_slug_identifier_is_refused() {
        assert!(WorkspaceTemplate::builder()
            .name("My Workspace")
            .slug("Not A Slug")
            .build()
            .is_err());
    }

    /// The display name is rendered without escaping into `mo.md(r"""…""")`, so a name
    /// that can close that literal produces a `readme.py` which is a Python syntax
    /// error. `readme.py` is the workspace's default overview notebook, so the
    /// workspace would be born unopenable — and the name comes from unvalidated user
    /// input at the platform's GraphQL boundary.
    #[test]
    fn a_name_that_could_break_the_notebook_is_refused() {
        for name in [
            r#"Sneaky """) + __import__("os").system("id") + mo.md(r"""#,
            r#"ends with a backslash \"#,
            "carriage\rreturn",
            "   ",
            "",
        ] {
            assert!(
                WorkspaceTemplate::builder()
                    .name(name)
                    .slug("my-workspace")
                    .build()
                    .is_err(),
                "should have been refused: {name:?}"
            );
        }
    }

    /// Names people actually use must still work — the guard is narrow on purpose.
    #[test]
    fn ordinary_display_names_are_accepted() {
        for name in [
            "My Workspace",
            "Ünïcodé — dashes, \"quotes\" & 'apostrophes'",
            "back\\slash in the middle",
            "100% coverage (v2)",
        ] {
            assert!(
                WorkspaceTemplate::builder()
                    .name(name)
                    .slug("my-workspace")
                    .build()
                    .is_ok(),
                "should have been accepted: {name:?}"
            );
        }
    }
}
