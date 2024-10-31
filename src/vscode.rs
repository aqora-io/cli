use crate::dirs::vscode_user_settings_file_path;
use crate::error::{self, Result};
use crate::file_utils::with_locked_file;
use futures::FutureExt;
use serde::{Deserialize, Serialize};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
pub struct UserVSCodeSettings {
    pub can_install_extensions: Option<bool>,
}

impl UserVSCodeSettings {
    pub async fn load() -> Result<Self> {
        with_locked_file(
            |file| {
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
            },
            vscode_user_settings_file_path().await?,
        )
        .await
    }

    pub async fn save(&self) -> Result<()> {
        let contents = serde_json::to_string_pretty(self)?;
        with_locked_file(
            |file| {
                async move {
                    file.rewind().await?;
                    file.write_all(contents.as_bytes()).await?;
                    file.set_len(contents.len() as u64).await?;
                    file.sync_all().await?;

                    Ok(())
                }
                .boxed()
            },
            vscode_user_settings_file_path().await?,
        )
        .await
    }

    pub fn can_install_extensions(&mut self, can_install: bool) -> &mut Self {
        self.can_install_extensions = Some(can_install);
        self
    }
}
