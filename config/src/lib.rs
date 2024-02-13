use serde::{de, ser, Deserialize, Serialize};
use std::{
    borrow::Cow, collections::HashMap, convert::Infallible, fmt, path::PathBuf, str::FromStr,
};
use thiserror::Error;

pub use pep440_rs::{self, Version};
pub use pyproject_toml::{self, BuildSystem, Contact, License, LicenseFiles, Project, ReadMe};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PyProject {
    pub build_system: Option<BuildSystem>,
    pub project: Option<Project>,
    pub tool: Option<Tools>,
}

impl PyProject {
    pub fn set_name(&mut self, name: impl ToString) {
        if let Some(project) = self.project.as_mut() {
            project.name = name.to_string();
        } else {
            self.project = Some(Project::new(name.to_string()));
        }
    }

    pub fn version(&self) -> Option<Version> {
        self.project
            .as_ref()
            .and_then(|project| project.version.to_owned())
    }

    pub fn aqora(&self) -> Option<&AqoraConfig> {
        self.tool.as_ref().and_then(|tool| tool.aqora.as_ref())
    }

    pub fn from_toml(s: impl AsRef<str>) -> Result<Self, toml::de::Error> {
        toml::from_str(s.as_ref())
    }

    pub fn toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tools {
    pub aqora: Option<AqoraConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AqoraConfig {
    UseCase(AqoraUseCaseConfig),
    Submission(AqoraSubmissionConfig),
}

impl AqoraConfig {
    pub fn is_use_case(&self) -> bool {
        matches!(self, AqoraConfig::UseCase(_))
    }

    pub fn is_submission(&self) -> bool {
        matches!(self, AqoraConfig::Submission(_))
    }

    pub fn as_use_case(&self) -> Option<&AqoraUseCaseConfig> {
        match self {
            AqoraConfig::UseCase(use_case) => Some(use_case),
            AqoraConfig::Submission(_) => None,
        }
    }

    pub fn as_submission(&self) -> Option<&AqoraSubmissionConfig> {
        match self {
            AqoraConfig::UseCase(_) => None,
            AqoraConfig::Submission(submission) => Some(submission),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AqoraUseCaseConfig {
    pub competition: String,
    pub data: PathBuf,
    pub template: Option<PathBuf>,
    pub generator: PathStr<'static>,
    pub aggregator: PathStr<'static>,
    pub context: Option<PathStr<'static>>,
    pub layers: Vec<LayerConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LayerConfig {
    pub name: String,
    pub transform: Option<FunctionDef>,
    pub metric: Option<FunctionDef>,
    pub branch: Option<FunctionDef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AqoraSubmissionConfig {
    pub competition: String,
    pub entity: Option<String>,
    pub refs: HashMap<String, PathStr<'static>>,
}

#[derive(Clone, Serialize, Debug)]
pub struct FunctionDef {
    pub path: PathStr<'static>,
    pub context: bool,
}

impl<'de> Deserialize<'de> for FunctionDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct FunctionDefVisitor;

        impl<'de> de::Visitor<'de> for FunctionDefVisitor {
            type Value = FunctionDef;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid function definition")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FunctionDef {
                    path: value.parse().map_err(de::Error::custom)?,
                    context: false,
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut path = None;
                let mut context = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "path" => {
                            if path.is_some() {
                                return Err(de::Error::duplicate_field("path"));
                            }
                            path = Some(map.next_value()?);
                        }
                        "context" => {
                            if context.is_some() {
                                return Err(de::Error::duplicate_field("context"));
                            }
                            context = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(de::Error::unknown_field(
                                key.as_str(),
                                &["path", "context"],
                            ));
                        }
                    }
                }
                let path = path.ok_or_else(|| de::Error::missing_field("path"))?;
                Ok(FunctionDef {
                    path,
                    context: context.unwrap_or(false),
                })
            }
        }

        deserializer.deserialize_any(FunctionDefVisitor)
    }
}

#[derive(Clone)]
pub struct PathStr<'a>(Cow<'a, [String]>);

#[derive(Error, Debug)]
pub enum PathStrReplaceError {
    #[error("Ref not found: {0}")]
    RefNotFound(String),
}

impl<'a> PathStr<'a> {
    pub fn module<'b: 'a>(&'b self) -> PathStr<'b> {
        Self(Cow::Borrowed(&self.0[..self.0.len() - 1]))
    }
    pub fn name(&self) -> &str {
        self.0.last().unwrap()
    }
    pub fn replace_refs(
        &self,
        refs: &HashMap<String, PathStr>,
    ) -> Result<PathStr<'static>, PathStrReplaceError> {
        let mut out = Vec::new();
        for part in self.0.iter() {
            if let Some(ref_key) = part.strip_prefix('$') {
                if let Some(replacement) = refs.get(ref_key) {
                    out.extend(replacement.0.iter().cloned());
                } else {
                    return Err(PathStrReplaceError::RefNotFound(ref_key.to_string()));
                }
            } else {
                out.push(part.clone());
            }
        }
        Ok(PathStr(Cow::Owned(out)))
    }
}

impl fmt::Display for PathStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

impl fmt::Debug for PathStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PathStr").field(&self.0.join(".")).finish()
    }
}

impl ser::Serialize for PathStr<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.0.join("."))
    }
}

impl FromStr for PathStr<'static> {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(PathStr(s.split('.').map(|s| s.to_string()).collect()))
    }
}

impl<'de> de::Deserialize<'de> for PathStr<'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct PathStrVisitor;

        impl<'de> de::Visitor<'de> for PathStrVisitor {
            type Value = PathStr<'static>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                value.parse().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(PathStrVisitor)
    }
}
