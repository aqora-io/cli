use crate::{
    commands::GlobalArgs,
    dirs::{project_config_dir, project_venv_dir, project_venv_symlink_path},
};
use clap::Args;
use glob::glob;
use owo_colors::{OwoColorize, Stream as OwoStream};

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Clean;

pub async fn clean(_: Clean, global: GlobalArgs) -> crate::error::Result<()> {
    let project_config_dir = project_config_dir(&global.project);
    if project_config_dir.exists() {
        if let Err(err) = tokio::fs::remove_dir_all(&project_config_dir).await {
            eprintln!(
                "{}: Failed to remove project config directory at {}: {}",
                "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                project_config_dir.display(),
                err
            );
        }
    }
    let venv_symlink = project_venv_symlink_path(&global.project);
    if venv_symlink.exists() {
        if let Err(err) = tokio::fs::remove_file(&venv_symlink).await {
            eprintln!(
                "{}: Failed to remove project venv symlink at {}: {}",
                "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                venv_symlink.display(),
                err
            );
        }
    }
    let venv_dir = project_venv_dir(&global.project);
    if venv_dir.exists() {
        if let Err(err) = tokio::fs::remove_dir_all(&venv_dir).await {
            eprintln!(
                "{}: Failed to remove project venv directory at {}: {}",
                "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                venv_dir.display(),
                err
            );
        }
    }
    for entry in glob("**/*.egg-info")
        .expect("{}: Failed to read glob pattern")
        .flatten()
    {
        if entry.is_dir() {
            if let Err(err) = tokio::fs::remove_dir_all(&entry).await {
                eprintln!(
                    "{}: Failed to remove egg-info directory at {}: {}",
                    "WARNING".if_supports_color(OwoStream::Stderr, |t| t.yellow()),
                    entry.display(),
                    err
                );
            }
        }
    }
    Ok(())
}
