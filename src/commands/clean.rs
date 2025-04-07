use crate::{
    commands::GlobalArgs,
    dirs::{project_config_dir, project_venv_dir, read_pyproject},
    error::{self, Result},
};
use clap::Args;
use serde::Serialize;
use std::path::Path;

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Clean;

async fn clean_dir(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let project_config_dir = project_config_dir(path);
    if project_config_dir.exists() {
        if let Err(err) = tokio::fs::remove_dir_all(&project_config_dir).await {
            tracing::warn!(
                "Failed to remove project config directory at {}: {}",
                project_config_dir.display(),
                err
            );
        }
    }
    let venv_dir = project_venv_dir(path);
    if venv_dir.exists() {
        if venv_dir.is_symlink() {
            if let Err(err) = tokio::fs::remove_file(&venv_dir).await {
                tracing::warn!(
                    "Failed to remove project venv symlink at {}: {}",
                    venv_dir.display(),
                    err
                );
            }
        } else if let Err(err) = tokio::fs::remove_dir_all(&venv_dir).await {
            tracing::warn!(
                "Failed to remove project venv directory at {}: {}",
                venv_dir.display(),
                err
            );
        }
    }
    let gitignore = {
        let mut builder = ignore::gitignore::GitignoreBuilder::new(path);
        if let Some(err) = builder.add(path.join(".gitignore")) {
            Err(err)
        } else {
            builder.build()
        }
    };
    if let Ok(gitignore) = gitignore {
        for entry in ignore::WalkBuilder::new(path)
            .standard_filters(false)
            .build()
            .flatten()
            .map(|entry| entry.into_path())
        {
            if !gitignore
                .matched_path_or_any_parents(&entry, entry.is_dir())
                .is_ignore()
            {
                continue;
            }
            if entry.is_dir()
                && (entry.extension().is_some_and(|ext| ext == "egg-info")
                    || entry
                        .file_name()
                        .is_some_and(|name| name == "__pycache__" || name == "__aqora__"))
            {
                if let Err(err) = tokio::fs::remove_dir_all(&entry).await {
                    tracing::warn!("Failed to remove directory at {}: {}", entry.display(), err);
                }
            } else if entry.is_file()
                && entry
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| matches!(ext, "pyc" | "pyo" | "pyd" | "egg"))
            {
                if let Err(err) = tokio::fs::remove_file(&entry).await {
                    tracing::warn!("Failed to remove file at {}: {}", entry.display(), err);
                }
            }
        }
    }
    Ok(())
}

pub async fn clean(_: Clean, global: GlobalArgs) -> Result<()> {
    let project = read_pyproject(&global.project).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;
    if let Some(template) = aqora
        .as_use_case()
        .and_then(|aqora| aqora.template.as_ref())
    {
        clean_dir(template).await?;
    }
    clean_dir(&global.project).await?;
    Ok(())
}
