use crate::dirs::vscode_user_settings_file_path;
use crate::error::{self, Result};
use fs4::tokio::AsyncFileExt;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct UserVSCodeSettings {
    pub can_install_extensions: Option<bool>,
}

impl UserVSCodeSettings {
    pub async fn load() -> Result<Self> {
        with_locked_settings(|file| {
            async move {
                let mut contents = String::new();
                file.read_to_string(&mut contents).await?;

                let settings = serde_json::from_str(&contents)
                    .map_err(|e| {
                        error::system(
                            &format!("Failed to parse user vscode settings file: {:?}", e),
                            "",
                        )
                    })
                    .unwrap_or(UserVSCodeSettings {
                        can_install_extensions: None,
                    });

                Ok(settings)
            }
            .boxed()
        })
        .await
    }

    pub async fn save(&self) -> Result<()> {
        let contents = serde_json::to_string_pretty(self)?;
        with_locked_settings(|file| {
            async move {
                file.rewind().await?;
                file.write_all(contents.as_bytes()).await?;
                file.set_len(contents.len() as u64).await?;
                file.sync_all().await?;

                Ok(())
            }
            .boxed()
        })
        .await
    }

    pub fn can_install_extensions(&mut self, can_install: bool) -> &mut Self {
        self.can_install_extensions = Some(can_install);
        self
    }
}

async fn with_locked_settings<T, F>(f: F) -> Result<T>
where
    F: for<'a> FnOnce(&'a mut File) -> BoxFuture<'a, Result<T>> + Send,
    T: Send,
{
    let path = vscode_user_settings_file_path().await?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await
        .map_err(|e| {
            error::system(
                &format!(
                    "Failed to open user vscode settings file at {}: {:?}",
                    path.display(),
                    e
                ),
                "",
            )
        })?;

    file.lock_exclusive().map_err(|e| {
        error::system(
            &format!(
                "Failed to lock user vscode settings file at {}: {:?}",
                path.display(),
                e
            ),
            "",
        )
    })?;

    let res = f(&mut file).await;

    file.unlock().map_err(|e| {
        error::system(
            &format!(
                "Failed to unlock user vscode settings file at {}: {:?}",
                path.display(),
                e
            ),
            "",
        )
    })?;

    res
}
