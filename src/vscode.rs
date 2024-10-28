use dirs::config_dir;
use fs4::tokio::AsyncFileExt;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

const AQORA_CAN_INSTALL_EXT_KEY: &str = "aqora.canInstallExtensions";
const VSCODE_SETTINGS_FILENAME: &str = "settings.json";

#[derive(Debug, Serialize, Deserialize)]
pub struct VSCodeSettings {
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

// Sometimes, users may provide JSON with a trailing comma,
// which serde does not support.
fn clean_json(input: impl AsRef<str>) -> String {
    let re = Regex::new(r",\s*(\}|\])").unwrap();
    re.replace_all(input.as_ref(), "$1").to_string()
}

impl VSCodeSettings {
    pub async fn load() -> Result<Self, std::io::Error> {
        let path = settings_file_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await?;

        let _ = file.lock_exclusive();

        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;
        let settings: VSCodeSettings = if contents.is_empty() {
            VSCodeSettings {
                other: HashMap::new(),
            }
        } else {
            serde_json::from_str(&clean_json(&contents))?
        };

        let _ = file.unlock();
        Ok(settings)
    }

    pub async fn save(&self) -> Result<(), std::io::Error> {
        let path = settings_file_path();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await?;

        let _ = file.lock_exclusive();

        let contents = serde_json::to_string_pretty(&self)?;
        file.rewind().await?;
        file.write_all(contents.as_bytes()).await?;
        file.set_len(contents.len() as u64).await?;
        file.sync_all().await?;

        let _ = file.unlock();
        Ok(())
    }

    pub async fn set_aqora_can_install_extensions(&mut self, value: bool) {
        self.other
            .insert(AQORA_CAN_INSTALL_EXT_KEY.to_string(), json!(value));
    }

    pub fn aqora_can_install_extensions(&self) -> Option<bool> {
        self.other
            .get(AQORA_CAN_INSTALL_EXT_KEY)
            .and_then(|can_install| serde_json::from_value::<bool>(can_install.clone()).ok())
    }
}

fn settings_file_path() -> PathBuf {
    config_dir()
        .unwrap()
        .join("Code")
        .join("User")
        .join(VSCODE_SETTINGS_FILENAME)
}
