use crate::error::{self, Result};
use aqora_config::PyProject;
use aqora_runner::python::PyEnv;
use std::path::{Path, PathBuf};

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
    project_dir.as_ref().join(".aqora")
}

pub fn project_venv_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_config_dir(project_dir).join("venv")
}

pub fn project_data_dir(project_dir: impl AsRef<Path>, kind: impl ToString) -> PathBuf {
    project_config_dir(project_dir)
        .join("data")
        .join(kind.to_string())
}

pub fn pyproject_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join("pyproject.toml")
}

pub fn project_use_case_toml_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_data_dir(project_dir, "use_case.toml")
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

pub async fn init_venv(project_dir: impl AsRef<Path>) -> Result<PyEnv> {
    let venv_dir = project_venv_dir(&project_dir);
    PyEnv::init(&venv_dir).await.map_err(|e| {
        error::user(
            &format!("Failed to setup virtualenv: {}", e),
            &format!(
                "Please make sure you have permissions to write to {}",
                venv_dir.display()
            ),
        )
    })
}
