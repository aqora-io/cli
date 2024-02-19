use crate::error::{self, Result};
use aqora_config::PyProject;
use aqora_runner::python::PyEnv;
use std::path::{Path, PathBuf};
use thiserror::Error;

const AQORA_DIRNAME: &str = ".aqora";
const DATA_DIRNAME: &str = "data";
const VENV_DIRNAME: &str = "venv";
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
    let symlink_dir = project_dir.as_ref().join(VENV_DIRNAME);
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

pub async fn init_venv(project_dir: impl AsRef<Path>) -> Result<PyEnv> {
    let venv_dir = project_venv_dir(&project_dir);
    let env = PyEnv::init(&venv_dir).await.map_err(|e| {
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
