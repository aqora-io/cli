use crate::{
    error::{self, Result},
    process::run_command,
};
use aqora_config::PyProject;
use aqora_runner::python::PyEnv;
use indicatif::ProgressBar;
use std::path::{Path, PathBuf};
use thiserror::Error;

const AQORA_DIRNAME: &str = ".aqora";
const DATA_DIRNAME: &str = "data";
const VENV_DIRNAME: &str = "venv";
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
    project_config_dir(project_dir).join(VENV_DIRNAME)
}

pub fn project_last_run_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_config_dir(project_dir).join(LAST_RUN_DIRNAME)
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

#[derive(Debug, Error)]
enum SymlinkError {
    #[error("Failed to create symlink from {0} to {1}: {2}")]
    CreateSymlink(PathBuf, PathBuf, std::io::Error),
    #[error("{0} directory already exists. Symlink to {1} could not be created.")]
    VenvDirExists(PathBuf, PathBuf),
}

fn create_venv_symlink(project_dir: impl AsRef<Path>) -> Result<(), SymlinkError> {
    let symlink_dir = project_dir.as_ref().join(".venv");
    let venv_dir = [AQORA_DIRNAME, VENV_DIRNAME].iter().collect::<PathBuf>();

    if symlink_dir.exists() {
        if symlink_dir.read_link().ok().as_ref() != Some(&venv_dir) {
            return Err(SymlinkError::VenvDirExists(symlink_dir, venv_dir));
        }
        return Ok(());
    }

    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    #[cfg(windows)]
    use std::os::windows::fs::symlink_dir as symlink;

    symlink(&venv_dir, &symlink_dir)
        .map_err(|err| SymlinkError::CreateSymlink(symlink_dir, venv_dir, err))
}

async fn ensure_uv(uv_path: Option<impl AsRef<Path>>, pb: &ProgressBar) -> Result<PathBuf> {
    if let Some(uv_path) = uv_path
        .map(|p| PathBuf::from(p.as_ref()))
        .or_else(|| which::which("uv").ok())
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
) -> Result<PyEnv> {
    pb.set_message("Initializing the Python environment...");
    let uv_path = ensure_uv(uv_path, pb).await?;
    let venv_dir = project_venv_dir(&project_dir);
    let env = PyEnv::init(uv_path, &venv_dir, None::<PathBuf>)
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
    if let Err(err) = create_venv_symlink(&project_dir) {
        eprintln!("WARN: {err}");
    }
    Ok(env)
}
