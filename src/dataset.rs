use std::{collections::HashMap, fmt::Display, num::ParseIntError, path::PathBuf, str::FromStr};

use aqora_data_utils::Schema;
use serde::{de::Visitor, Deserialize, Serialize};
use thiserror::Error;

use crate::commands::data::FormatOptions;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DatasetRootConfig {
    pub aqora: AqoraDatasetConfig,
}

impl DatasetRootConfig {
    pub fn from_toml(toml: impl AsRef<str>) -> Result<Self, toml::de::Error> {
        toml::from_str(toml.as_ref())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AqoraDatasetConfig {
    pub dataset: HashMap<String, DatasetConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub version: DatasetVersion,
    pub path: PathBuf,
    pub authors: Vec<DatasetAuthorConfig>,
    pub schema: Schema,
    pub format: FormatOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetAuthorConfig {
    Name(String),
    Contact { name: String, email: String },
}

#[derive(Clone, Copy, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct DatasetVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl DatasetVersion {
    pub const ONE: Self = Self {
        major: 1,
        minor: 0,
        patch: 0,
    };

    pub fn next_patch(self) -> Self {
        Self {
            patch: self.patch + 1,
            ..self
        }
    }
}

impl Display for DatasetVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}.{}.{}", self.major, self.minor, self.patch))
    }
}

#[derive(Debug, Error)]
pub enum DatasetVersionParseError {
    #[error(transparent)]
    InvalidInteger(#[from] ParseIntError),
    #[error("Missing major")]
    MissingMajor,
    #[error("Missing minor")]
    MissingMinor,
    #[error("Missing patch")]
    MissingPatch,
}

impl FromStr for DatasetVersion {
    type Err = DatasetVersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DatasetVersionParseError::*;
        let mut parts = s.split('.').map(u32::from_str);
        let major = parts.next().transpose()?.ok_or(MissingMajor)?;
        let minor = parts.next().transpose()?.ok_or(MissingMinor)?;
        let patch = parts.next().transpose()?.ok_or(MissingPatch)?;
        Ok(Self {
            major,
            minor,
            patch,
        })
    }
}

impl Serialize for DatasetVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DatasetVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct _Visitor;
        impl Visitor<'_> for _Visitor {
            type Value = DatasetVersion;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Semver version")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                DatasetVersion::from_str(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(_Visitor)
    }
}
