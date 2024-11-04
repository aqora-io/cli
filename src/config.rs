use std::io;
use std::path::{Path, PathBuf};

use futures::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use toml_edit::DocumentMut;

use crate::dirs::project_config_file_path;
use crate::error::{system, user, Error};

lazy_static::lazy_static! {
static ref DEFAULT_TEMPLATE: DocumentMut = r#"# Project configuration

# The default configuration set by the competition
[default]

# User specific overrides
[user]
"#.parse::<DocumentMut>().unwrap();
}

fn merge_toml_value(left: toml::Value, right: toml::Value) -> toml::Value {
    match (left, right) {
        (toml::Value::Table(mut left), toml::Value::Table(right)) => {
            for (key, right_value) in right {
                if let Some(left_value) = left.remove(&key) {
                    left.insert(key, merge_toml_value(left_value, right_value));
                } else {
                    left.insert(key, right_value);
                }
            }
            toml::Value::Table(left)
        }
        (_, right) => right,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigFile {
    default: Option<toml::Value>,
    user: Option<toml::Value>,
}

impl ConfigFile {
    fn merged(self) -> toml::Value {
        match (self.default, self.user) {
            (Some(default), Some(user)) => merge_toml_value(default, user),
            (None, Some(user)) => user,
            (Some(default), None) => default,
            (None, None) => toml::Value::Table(Default::default()),
        }
    }

    fn try_into<T>(self) -> Result<T, toml::de::Error>
    where
        T: DeserializeOwned,
    {
        self.merged().try_into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProjectConfig {
    pub show_score: bool,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self { show_score: true }
    }
}

#[derive(Debug, Error)]
pub enum ReadProjectConfigError {
    #[error("Could not read project configuration file '{0}': {1}")]
    Io(PathBuf, #[source] io::Error),
    #[error("Project configuration file '{0}' is invalid: {1}")]
    Invalid(PathBuf, #[source] toml::de::Error),
}

impl From<ReadProjectConfigError> for Error {
    fn from(value: ReadProjectConfigError) -> Self {
        match &value {
            ReadProjectConfigError::Io(..) => system(
                &value.to_string(),
                "Check that the file exists and you have permissions to read it",
            ),
            ReadProjectConfigError::Invalid(..) => {
                user(&value.to_string(), "Make sure the file is valid toml")
            }
        }
    }
}
async fn read_config_file<T>(path: impl AsRef<Path>) -> Result<T, ReadProjectConfigError>
where
    T: DeserializeOwned + Default,
{
    let path = path.as_ref();
    if tokio::fs::try_exists(path)
        .await
        .map_err(|e| ReadProjectConfigError::Io(path.to_path_buf(), e))?
    {
        let string = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ReadProjectConfigError::Io(path.to_path_buf(), e))?;
        let file: ConfigFile = toml::from_str(&string)
            .map_err(|e| ReadProjectConfigError::Invalid(path.to_path_buf(), e))?;
        file.try_into()
            .map_err(|e| ReadProjectConfigError::Invalid(path.to_path_buf(), e))
    } else {
        Ok(Default::default())
    }
}

pub async fn read_project_config(
    project_dir: impl AsRef<Path>,
) -> Result<ProjectConfig, ReadProjectConfigError> {
    read_config_file(project_config_file_path(project_dir)).await
}

#[derive(Debug, Error)]
pub enum WriteProjectConfigError {
    #[error("Could not read project configuration file '{0}': {1}")]
    ReadIo(PathBuf, #[source] io::Error),
    #[error("Could not write project configuration file '{0}': {1}")]
    WriteIo(PathBuf, #[source] io::Error),
    #[error("Invalid project configuration file '{0}': {1}")]
    InvalidExisting(PathBuf, #[source] toml_edit::TomlError),
    #[error("Invalid new configuration: {0}")]
    InvalidNew(#[source] toml_edit::ser::Error),
}

impl From<WriteProjectConfigError> for Error {
    fn from(value: WriteProjectConfigError) -> Self {
        match &value {
            WriteProjectConfigError::ReadIo(..) => system(
                &value.to_string(),
                "Check that the you have permissions to read it",
            ),
            WriteProjectConfigError::WriteIo(..) => system(
                &value.to_string(),
                "Check that the you have permissions to write to it",
            ),
            WriteProjectConfigError::InvalidExisting(..) => {
                user(&value.to_string(), "Make sure the file is valid toml")
            }
            WriteProjectConfigError::InvalidNew(..) => {
                user(&value.to_string(), "Make sure the new config is valid")
            }
        }
    }
}

async fn write_config_file_default<T>(
    path: impl AsRef<Path>,
    config: &T,
) -> Result<(), WriteProjectConfigError>
where
    T: Serialize,
{
    let path = path.as_ref();
    let config_value =
        toml_edit::ser::to_document(&config).map_err(WriteProjectConfigError::InvalidNew)?;
    let mut doc = if let Some(doc) = tokio::fs::try_exists(path)
        .and_then(|exists| async move {
            Ok(if exists {
                let doc = tokio::fs::read_to_string(path).await?;
                if doc.trim().is_empty() {
                    None
                } else {
                    Some(doc)
                }
            } else {
                None
            })
        })
        .await
        .map_err(|e| WriteProjectConfigError::ReadIo(path.to_path_buf(), e))?
    {
        doc.parse::<DocumentMut>()
            .map_err(|e| WriteProjectConfigError::InvalidExisting(path.to_path_buf(), e))?
    } else {
        DEFAULT_TEMPLATE.clone()
    };
    if let Some(value) = doc.get_mut("default") {
        if let Some(table) = value.as_table_mut() {
            table.extend(config_value.as_table())
        } else {
            *value = config_value.as_item().clone()
        }
    } else {
        doc.insert("default", config_value.as_item().clone());
    }
    tokio::fs::write(path, doc.to_string())
        .await
        .map_err(|e| WriteProjectConfigError::WriteIo(path.to_path_buf(), e))?;
    Ok(())
}

pub async fn write_project_config_default(
    project_dir: impl AsRef<Path>,
    config: &ProjectConfig,
) -> Result<(), WriteProjectConfigError> {
    write_config_file_default(project_config_file_path(project_dir), config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[derive(Debug, Serialize, Deserialize, Default)]
    #[serde(default)]
    pub struct ExampleConfig {
        setting: bool,
    }

    #[tokio::test]
    async fn test_write_config() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        write_config_file_default(file.path(), &ExampleConfig { setting: false }).await?;
        let written = tokio::fs::read_to_string(file.path()).await?;
        assert_eq!(
            written,
            r#"# Project configuration

# The default configuration set by the competition
[default]
setting = false

# User specific overrides
[user]
"#
        );
        write_config_file_default(file.path(), &ExampleConfig { setting: true }).await?;
        let written = tokio::fs::read_to_string(file.path()).await?;
        assert_eq!(
            written,
            r#"# Project configuration

# The default configuration set by the competition
[default]
setting = true

# User specific overrides
[user]
"#
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_config() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        assert!(
            !read_config_file::<ExampleConfig>(file.path())
                .await?
                .setting
        );
        tokio::fs::write(
            file.path(),
            r#"# Project configuration

# The default configuration set by the competition
[default]
setting = true

# User specific overrides
[user]
"#,
        )
        .await?;
        assert!(
            read_config_file::<ExampleConfig>(file.path())
                .await?
                .setting
        );
        tokio::fs::write(
            file.path(),
            r#"# Project configuration

# The default configuration set by the competition
[default]
setting = true

# User specific overrides
[user]
setting = false
"#,
        )
        .await?;
        assert!(
            !read_config_file::<ExampleConfig>(file.path())
                .await?
                .setting
        );
        Ok(())
    }
}
