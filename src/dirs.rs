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
use pyo3::Python;
use std::path::{Path, PathBuf};

const AQORA_DIRNAME: &str = ".aqora";
const DATA_DIRNAME: &str = "data";
const VENV_DIRNAME: &str = ".venv";
const LAST_RUN_DIRNAME: &str = "last_run";
const PYPROJECT_FILENAME: &str = "pyproject.toml";
const USE_CASE_FILENAME: &str = "use_case.toml";

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

pub fn project_venv_symlink_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join(".venv")
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

pub fn locate_uv(uv_path: Option<impl AsRef<Path>>) -> Option<PathBuf> {
    if let Some(uv_path) = uv_path.as_ref().map(|p| p.as_ref()).filter(|p| p.exists()) {
        Some(PathBuf::from(uv_path))
    } else if let Ok(path) = which::which("uv") {
        Some(path)
    } else {
        let mut pipx_home_dirs = vec![];
        if let Some(home) = std::env::var_os("HOME") {
            pipx_home_dirs.push(PathBuf::from(home).join(".local").join("pipx"));
        }
        if let Some(pipx_home) = std::env::var_os("PIPX_HOME") {
            pipx_home_dirs.push(PathBuf::from(pipx_home));
        }
        if let Ok(data_dir) = Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "platformdirs"))?
                .call_method0(pyo3::intern!(py, "user_data_dir"))?
                .extract::<String>()
        }) {
            pipx_home_dirs.push(PathBuf::from(data_dir).join("pipx"));
        }
        for pipx_home in pipx_home_dirs {
            let uv_path = pipx_home
                .join("venvs")
                .join(manifest_name())
                .join("bin")
                .join("uv");
            if uv_path.exists() {
                return Some(uv_path);
            }
        }
        None
    }
}

async fn ensure_uv(uv_path: Option<impl AsRef<Path>>, pb: &ProgressBar) -> Result<PathBuf> {
    if let Some(uv_path) = uv_path
        .as_ref()
        .map(|p| PathBuf::from(p.as_ref()))
        .or_else(|| locate_uv(uv_path))
    {
        return if uv_path.exists() {
            Ok(uv_path)
        } else {
            Err(error::user(
                &format!("`uv` executable not found at {}", uv_path.display()),
                "Please make sure you have the correct path to `uv`",
            ))
        };
    }

    let confirmation = pb.suspend(|| {
        dialoguer::Confirm::new()
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
    let uv_path = ensure_uv(uv_path, pb).await?;
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
