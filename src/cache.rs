use crate::{
    dirs::project_config_dir,
    error::{self, Result},
};
use chrono::{DateTime, FixedOffset, Utc};
use std::path::{Path, PathBuf};

fn project_updated_since(project_dir: impl AsRef<Path>, time: DateTime<FixedOffset>) -> bool {
    ignore::WalkBuilder::new(project_dir).build().any(|entry| {
        entry
            .as_ref()
            .ok()
            .and_then(|entry| entry.metadata().ok())
            .map(|meta| {
                meta.is_file()
                    && meta
                        .modified()
                        .ok()
                        .map(|t| chrono::DateTime::<Utc>::from(t) > time)
                        .unwrap_or(false)
            })
            .unwrap_or(false)
    })
}

fn last_update_path(project_dir: impl AsRef<Path>) -> PathBuf {
    project_config_dir(project_dir).join("last_update")
}

async fn get_last_update_time(
    project_dir: impl AsRef<Path>,
) -> Result<Option<DateTime<FixedOffset>>> {
    let last_update_path = last_update_path(project_dir);
    if !last_update_path.exists() {
        return Ok(None);
    }
    Ok(Some(
        chrono::DateTime::parse_from_rfc3339(&tokio::fs::read_to_string(last_update_path).await?)
            .map_err(|e| {
            error::system(
                &format!("Failed to read last update time: {e}"),
                "Try running `aqora install` again",
            )
        })?,
    ))
}

pub async fn needs_update(project_dir: impl AsRef<Path>) -> Result<bool> {
    if let Some(last_update) = get_last_update_time(&project_dir).await? {
        Ok(project_updated_since(&project_dir, last_update))
    } else {
        Ok(true)
    }
}

pub async fn set_last_update_time(project_dir: impl AsRef<Path>) -> Result<()> {
    let last_update_path = last_update_path(&project_dir);
    tokio::fs::create_dir_all(last_update_path.parent().unwrap())
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to write last-update: {e}"),
                &format!(
                    "Make sure you have permissions to write to {}",
                    last_update_path.parent().unwrap().display()
                ),
            )
        })?;
    tokio::fs::write(&last_update_path, Utc::now().to_rfc3339())
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to write last-update: {e}"),
                &format!(
                    "Make sure you have permissions to write to {}",
                    last_update_path.display()
                ),
            )
        })?;
    Ok(())
}
