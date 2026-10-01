use std::path::Path;

use derive_builder::Builder;
use handlebars::{RenderError, RenderErrorReason};
use serde::Serialize;

use crate::registry::REGISTRY;
use crate::utils::{
    assert_python_minor, assert_python_raw_string_safe, assert_semver, DEFAULT_PYTHON_VERSION,
};

const DEFAULT_MARIMO_VERSION: &str = "0.25.0";
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
            .map(assert_python_minor)
            .transpose()?;
        self.marimo_version
            .as_deref()
            .map(assert_semver)
            .transpose()?;
        self.version.as_deref().map(assert_semver).transpose()?;
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
    use crate::utils::extract_pep723_toml;

    fn rendered() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("workspace");
        WorkspaceTemplate::builder()
            .name("My Workspace")
            .render(&out)
            .expect("render");
        (dir, out)
    }

    #[test]
    fn renders_the_starter_files() {
        let (_dir, out) = rendered();
        for file in ["readme.py", ".ignore", ".secrets"] {
            assert!(out.join(file).is_file(), "missing {file}");
        }
        // `.ignore`, not `.gitignore`: a hosted workspace has no git, and the
        // indexer owns the exclusions. No `pyproject.toml`: kernels run in a
        // per-notebook environment, built by uv or pixi as the workspace's
        // runtime selects, from readme.py's own PEP 723 header, and an old
        // start.sh appends `[tool.marimo.venv]` to any pyproject it finds.
        assert!(!out.join(".gitignore").exists());
        assert!(!out.join("pyproject.toml").exists());
    }

    /// The scaffolded dotfiles document behavior without enacting any: an
    /// active pattern in `.secrets` would silently divert user files into the
    /// secrets side-channel from day one.
    #[test]
    fn the_secrets_file_ships_comments_only() {
        let (_dir, out) = rendered();
        let secrets = std::fs::read_to_string(out.join(".secrets")).unwrap();
        assert!(
            secrets
                .lines()
                .all(|line| line.trim().is_empty() || line.starts_with('#')),
            "{secrets}"
        );
        // The exclusions, by contrast, are live — the venv must never be
        // archived.
        let ignore = std::fs::read_to_string(out.join(".ignore")).unwrap();
        assert!(ignore.lines().any(|line| line == ".venv"), "{ignore}");
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

    /// kubimo's image pre-builds a Python environment for this exact header at
    /// `/home/me/workspace/readme.py`, so the bytes must match verbatim.
    #[test]
    fn the_readme_starts_with_the_canonical_header() {
        let (_dir, out) = rendered();
        let readme = std::fs::read_to_string(out.join("readme.py")).unwrap();
        assert!(
            readme.starts_with(concat!(
                "# /// script\n",
                "# requires-python = \"==3.12.*\"\n",
                "# dependencies = [\n",
                "#     \"marimo\",\n",
                "# ]\n",
                "# ///\n",
            )),
            "{readme}"
        );
    }

    /// Declaring marimo here would make `uv sync` install a second copy into
    /// the venv, shadowing the image's system build — a ~920MB duplicate that
    /// also leaves kernels on a different marimo from the server.
    #[test]
    fn the_header_declares_marimo_unpinned() {
        let (_dir, out) = rendered();
        let readme = std::fs::read_to_string(out.join("readme.py")).unwrap();
        let header = extract_pep723_toml(&readme);
        assert_eq!(header["requires-python"].as_str(), Some("==3.12.*"));
        let deps = header["dependencies"]
            .as_array()
            .expect("dependencies array");
        assert!(
            deps.iter().any(|dep| dep.as_str() == Some("marimo")),
            "{deps:?}"
        );
    }

    #[test]
    fn no_rendered_file_contains_the_venv_table() {
        let (_dir, out) = rendered();
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
                WorkspaceTemplate::builder().name(name).build().is_err(),
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
                WorkspaceTemplate::builder().name(name).build().is_ok(),
                "should have been accepted: {name:?}"
            );
        }
    }

    /// The version lands in `requires-python = "==3.X.*"`, and Python's
    /// version specifiers take ASCII digits only.
    #[test]
    fn a_python_version_needs_ascii_digits() {
        let build = |version: &str| {
            WorkspaceTemplate::builder()
                .name("My Workspace")
                .python_version(version)
                .build()
        };
        assert!(build("3.13").is_ok());
        for version in ["3.\u{0661}\u{0662}", "3.\u{ff11}\u{ff12}", "3", "3.12.1"] {
            assert!(
                build(version).is_err(),
                "should have been refused: {version:?}"
            );
        }
    }
}
