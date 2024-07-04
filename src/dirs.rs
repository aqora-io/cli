use crate::{
    colors::ColorChoiceExt,
    error::{self, Result},
    manifest::manifest_name,
    process::run_command,
};
use aqora_config::PyProject;
use aqora_runner::python::PyEnv;
use clap::ColorChoice;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

const AQORA_DIRNAME: &str = ".aqora";
const DATA_DIRNAME: &str = "data";
const VENV_DIRNAME: &str = ".venv";
const VSCODE_DIRNAME: &str = ".vscode";
const LAST_RUN_DIRNAME: &str = "last_run";
const PYPROJECT_FILENAME: &str = "pyproject.toml";
const USE_CASE_FILENAME: &str = "use_case.toml";
const VSCODE_SETTINGS_FILENAME: &str = "settings.json";

pub async fn config_dir() -> Result<PathBuf> {
    let mut path = dirs::data_dir().or_else(dirs::config_dir).ok_or_else(|| {
        error::system(
            "Could not find config directory",
            "This is a bug, please report it",
        )
    })?;
    path.push("aqora");
    tokio::fs::create_dir_all(&path).await.map_err(|e| {
        error::system(
            &format!(
                "Failed to create config directory at {}: {:?}",
                path.display(),
                e
            ),
            "",
        )
    })?;
    Ok(path)
}

pub fn project_config_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join(AQORA_DIRNAME)
}

pub fn project_venv_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join(VENV_DIRNAME)
}

pub fn project_last_run_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_config_dir(project_dir).join(LAST_RUN_DIRNAME)
}

pub fn project_last_run_result(project_dir: impl AsRef<Path>) -> PathBuf {
    project_last_run_dir(project_dir).join("result.msgpack")
}

pub fn project_data_dir(project_dir: impl AsRef<Path>, kind: impl ToString) -> PathBuf {
    project_config_dir(project_dir)
        .join(DATA_DIRNAME)
        .join(kind.to_string())
}

pub fn pyproject_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join(PYPROJECT_FILENAME)
}

pub fn project_use_case_toml_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_data_dir(project_dir, USE_CASE_FILENAME)
}

pub fn project_vscode_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join(VSCODE_DIRNAME)
}

pub fn vscode_settings_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_vscode_dir(project_dir).join(VSCODE_SETTINGS_FILENAME)
}

pub async fn read_pyproject(project_dir: impl AsRef<Path>) -> Result<PyProject> {
    let path = pyproject_path(&project_dir);
    if !path.exists() {
        return Err(error::user(
            &format!("No pyproject.toml found at {}", path.display()),
            "Please make sure you are in the correct directory",
        ));
    }
    let string = tokio::fs::read_to_string(&path).await.map_err(|e| {
        error::user(
            &format!("Failed to read {}: {}", path.display(), e),
            &format!("Make sure you have permissions to read {}", path.display()),
        )
    })?;
    PyProject::from_toml(string).map_err(|e| {
        error::user(
            &format!("Failed to parse {}: {}", path.display(), e),
            "Please make sure the file is valid toml",
        )
    })
}

#[derive(Serialize, Deserialize, Debug)]
struct PipxAppPath {
    #[serde(rename = "__Path__")]
    path: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct PipxVenvPackageMetadata {
    app_paths_of_dependencies: Option<HashMap<String, Vec<PipxAppPath>>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct PipxVenvMetadata {
    main_package: Option<PipxVenvPackageMetadata>,
}

#[derive(Serialize, Deserialize, Debug)]
struct PipxVenv {
    metadata: Option<PipxVenvMetadata>,
}

#[derive(Serialize, Deserialize, Debug)]
struct PipxList {
    pipx_spec_version: String,
    venvs: HashMap<String, PipxVenv>,
}

pub async fn locate_uv(uv_path: Option<impl AsRef<Path>>) -> Option<PathBuf> {
    if let Some(uv_path) = uv_path.as_ref().map(|p| p.as_ref()).filter(|p| p.exists()) {
        Some(PathBuf::from(uv_path))
    } else if let Ok(path) = which::which("uv") {
        Some(path)
    } else {
        let out = serde_json::from_slice::<PipxList>(
            &tokio::process::Command::new("pipx")
                .arg("list")
                .arg("--json")
                .output()
                .await
                .ok()?
                .stdout,
        )
        .ok()?;
        for path in out
            .venvs
            .get(manifest_name())?
            .metadata
            .as_ref()?
            .main_package
            .as_ref()?
            .app_paths_of_dependencies
            .as_ref()?
            .get("uv")?
            .iter()
            .filter_map(|p| p.path.as_ref())
        {
            if Path::new(path).exists() {
                return Some(PathBuf::from(path));
            }
        }
        None
    }
}

async fn ensure_uv(
    uv_path: Option<impl AsRef<Path>>,
    pb: &ProgressBar,
    color: ColorChoice,
) -> Result<PathBuf> {
    if let Some(uv_path) = uv_path.as_ref() {
        return if uv_path.as_ref().exists() {
            Ok(uv_path.as_ref().into())
        } else {
            Err(error::user(
                &format!(
                    "`uv` executable not found at {}",
                    uv_path.as_ref().display()
                ),
                "Please make sure you have the correct path to `uv`",
            ))
        };
    }
    if let Some(uv_path) = locate_uv(uv_path).await {
        return Ok(uv_path);
    }

    let confirmation = pb.suspend(|| {
        dialoguer::Confirm::with_theme(color.dialoguer().as_ref())
            .with_prompt("`uv` is required. Install it now? (python3 -m pip install uv)")
            .interact()
    })?;

    if confirmation {
        let mut cmd = tokio::process::Command::new("python3");
        cmd.arg("-m").arg("pip").arg("install").arg("uv");
        run_command(&mut cmd, pb, Some("Installing `uv`"))
            .await
            .map_err(|e| {
                error::system(
                    &format!("Failed to install `uv`: {}", e),
                    "Please try installing `uv` manually",
                )
            })?;
        let uv_path = which::which("uv").map_err(|e| {
            error::system(
                &format!("Failed to find `uv` after installing: {}", e),
                "Please make sure uv is in your PATH",
            )
        })?;
        Ok(uv_path)
    } else {
        Err(error::user(
            "`uv` not found",
            "Please install uv and try again",
        ))
    }
}

pub async fn init_venv(
    project_dir: impl AsRef<Path>,
    uv_path: Option<impl AsRef<Path>>,
    pb: &ProgressBar,
    color: ColorChoice,
) -> Result<PyEnv> {
    pb.set_message("Initializing the Python environment...");
    let uv_path = ensure_uv(uv_path, pb, color).await?;
    let venv_dir = project_venv_dir(&project_dir);
    let env = PyEnv::init(uv_path, &venv_dir, None::<PathBuf>, color.pip())
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to setup virtualenv: {}", e),
                &format!(
                    "Please make sure you have permissions to write to {}",
                    venv_dir.display()
                ),
            )
        })?;
    Ok(env)
}
