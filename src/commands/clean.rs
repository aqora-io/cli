use crate::{
    commands::GlobalArgs,
    dirs::{project_config_dir, project_venv_dir},
};
use clap::Args;
use owo_colors::{OwoColorize, Stream as OwoStream};

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Clean;

pub async fn clean(_: Clean, global: GlobalArgs) -> crate::error::Result<()> {
    let project_config_dir = project_config_dir(&global.project);
    if project_config_dir.exists() {
        if let Err(err) = tokio::fs::remove_dir_all(&project_config_dir).await {
            tracing::error!(
                "{}: Failed to remove project config directory at {}: {}",
                "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                project_config_dir.display(),
                err
            );
        }
    }
    let venv_dir = project_venv_dir(&global.project);
    if venv_dir.exists() {
        if let Err(err) = tokio::fs::remove_dir_all(&venv_dir).await {
            tracing::error!(
                "{}: Failed to remove project venv directory at {}: {}",
                "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                venv_dir.display(),
                err
            );
        }
    }
    let gitignore = {
        let mut builder = ignore::gitignore::GitignoreBuilder::new(&global.project);
        if let Some(err) = builder.add(global.project.join(".gitignore")) {
            Err(err)
        } else {
            builder.build()
        }
    };
    if let Ok(gitignore) = gitignore {
        for entry in ignore::WalkBuilder::new(&global.project)
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
                && (entry.extension().map_or(false, |ext| ext == "egg-info")
                    || entry
                        .file_name()
                        .map_or(false, |name| name == "__pycache__"))
            {
                if let Err(err) = tokio::fs::remove_dir_all(&entry).await {
                    tracing::warn!(
                        "{}: Failed to remove directory at {}: {}",
                        "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                        entry.display(),
                        err
                    );
                }
            } else if entry.is_file()
                && (entry
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map_or(false, |ext| matches!(ext, "pyc" | "pyo" | "pyd" | "egg"))
                    || entry
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map_or(false, |name| {
                            name.starts_with("__generated__") && name.ends_with(".py")
                        }))
            {
                if let Err(err) = tokio::fs::remove_file(&entry).await {
                    tracing::warn!(
                        "{}: Failed to remove file at {}: {}",
                        "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                        entry.display(),
                        err
                    );
                }
            }
        }
    }
    Ok(())
}
