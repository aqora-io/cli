use crate::{
    error::{self, Error, Result},
    id::{Id, NodeType},
};
use pep440_rs::Version;
use pyo3::{
    types::{PyModule, PyString},
    PyAny, PyResult, Python,
};
use pyproject_toml::{BuildSystem, Project};
use serde::{de, ser, Deserialize, Serialize};
use std::{
    borrow::Cow,
    convert::Infallible,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct PyProject {
    pub build_system: Option<BuildSystem>,
    pub project: Option<Project>,
    pub tool: Option<Tools>,
}

impl PyProject {
    pub fn for_project(project_dir: impl AsRef<Path>) -> Result<Self> {
        let pyproject_path = project_dir.as_ref().join("pyproject.toml");
        toml::from_str(&std::fs::read_to_string(&pyproject_path).map_err(|err| {
            error::user(
                &format!("could not read {}: {}", pyproject_path.display(), err),
                "Please run this command in the root of your project or set the --project-dir flag",
            )
        })?)
        .map_err(|err| {
            error::user(
                &format!("could not read {}: {}", pyproject_path.display(), err),
                "Please make sure your pyproject.toml is valid",
            )
        })
    }

    pub fn name(&self) -> Result<PackageName> {
        self.project
            .as_ref()
            .map(|project| project.name.to_owned())
            .ok_or_else(|| {
                error::user(
                    "No name given",
                    "Make sure the name is set in the project section \
                        of your pyproject.toml and it matches the competition",
                )
            })?
            .parse()
    }

    pub fn version(&self) -> Result<Version> {
        self.project
            .as_ref()
            .and_then(|project| project.version.to_owned())
            .ok_or_else(|| {
                error::user(
                    "No version given",
                    "Make sure the version is set in the project section \
                        of your pyproject.toml",
                )
            })
    }

    pub fn aqora(&self) -> AqoraConfig {
        self.tool
            .as_ref()
            .and_then(|tool| tool.aqora.clone())
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tools {
    pub aqora: Option<AqoraConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AqoraConfig {
    pub data: Option<PathBuf>,
    pub generator: Option<FunctionDef>,
    pub aggregator: Option<FunctionDef>,
    #[serde(default)]
    pub layers: Vec<Layer>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Layer {
    pub evaluate: FunctionDef,
    pub metric: Option<FunctionDef>,
}

#[derive(Serialize, Debug, Clone)]
pub struct FunctionDef {
    pub path: PathStr<'static>,
}

impl<'de> de::Deserialize<'de> for FunctionDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct FunctionDefVisitor;

        impl<'de> de::Visitor<'de> for FunctionDefVisitor {
            type Value = FunctionDef;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FunctionDef {
                    path: value.parse().map_err(de::Error::custom)?,
                })
            }
        }

        deserializer.deserialize_str(FunctionDefVisitor)
    }
}

#[derive(Clone)]
pub struct PathStr<'a>(Cow<'a, [String]>);

impl<'a> PathStr<'a> {
    pub fn module<'b: 'a>(&'b self) -> PathStr<'b> {
        Self(Cow::Borrowed(&self.0[..self.0.len() - 1]))
    }
    pub fn name(&self) -> &str {
        self.0.last().unwrap()
    }
    pub fn import<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let module = PyModule::import(py, PyString::new(py, &self.module().to_string()))?;
        module.getattr(PyString::new(py, self.name()))
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

pub enum PackageName {
    UseCase { competition: Id },
    Submission { competition: Id, user: Id },
}

impl fmt::Display for PackageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PackageName::UseCase { competition } => {
                write!(f, "use-case-{}", competition.to_package_id())
            }
            PackageName::Submission { competition, user } => {
                write!(
                    f,
                    "submission-{}-{}",
                    competition.to_package_id(),
                    user.to_package_id()
                )
            }
        }
    }
}

impl FromStr for PackageName {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(package_id) = s.strip_prefix("use-case-") {
            Id::parse_package_id(package_id, NodeType::Competition)
                .map_err(|err| {
                    error::user(
                        &format!("Invalid package id: {}", err),
                        "Make sure the package id is valid",
                    )
                })
                .map(|competition| PackageName::UseCase { competition })
        } else if let Some(package_id) = s.strip_prefix("submission-") {
            if let Some((competition, user)) = package_id.split_once('-') {
                let competition = Id::parse_package_id(competition, NodeType::Competition)
                    .map_err(|err| {
                        error::user(
                            &format!("Invalid package id: {}", err),
                            "Make sure the package id is valid",
                        )
                    })?;
                let user = Id::parse_package_id(user, NodeType::User).map_err(|err| {
                    error::user(
                        &format!("Invalid package id: {}", err),
                        "Make sure the package id is valid",
                    )
                })?;
                Ok(PackageName::Submission { competition, user })
            } else {
                Err(error::user(
                    "Invalid package name: Missing user id",
                    "Make sure the package id is valid",
                ))
            }
        } else {
            Err(error::user(
                "Invalid package name",
                "Make sure the package name starts with either 'use-case' or 'submission'",
            ))
        }
    }
}
