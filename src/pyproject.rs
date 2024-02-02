use crate::{
    error::{self, Error, Result},
    id::{Id, NodeType},
};
use pep440_rs::Version;
use pyproject_toml::{BuildSystem, Project};
use serde::{de, ser, Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};
use tempfile::NamedTempFile;

pub fn project_data_dir(project_dir: impl AsRef<Path>) -> PathBuf {
    project_dir.as_ref().join(".aqora")
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PyProject {
    pub build_system: Option<BuildSystem>,
    pub project: Option<Project>,
    pub tool: Option<Tools>,
}

impl PyProject {
    pub fn for_project(project_dir: impl AsRef<Path>) -> Result<Self> {
        let pyproject_path = Self::path_for_project(project_dir)?;
        Self::from_toml(std::fs::read_to_string(&pyproject_path).map_err(|err| {
            error::user(
                &format!("Could not read {}: {}", pyproject_path.display(), err),
                "Please run this command in the root of your project or set the --project-dir flag",
            )
        })?)
    }

    pub fn from_toml(s: impl AsRef<str>) -> Result<Self> {
        toml::from_str(s.as_ref()).map_err(|err| {
            error::user(
                &format!("Could not read pyproject.toml: {}", err),
                "Please make sure your pyproject.toml is valid",
            )
        })
    }

    pub fn path_for_project(project_dir: impl AsRef<Path>) -> Result<PathBuf> {
        let pyproject_path = project_dir.as_ref().join("pyproject.toml");
        if !pyproject_path.exists() {
            return Err(error::user(
                &format!("Could not find {}", pyproject_path.display()),
                "Please run this command in the root of your project or set the --project-dir flag",
            ));
        }
        Ok(pyproject_path)
    }

    pub fn name(&self) -> Option<&str> {
        self.project.as_ref().map(|project| project.name.as_str())
    }

    pub fn set_name(&mut self, name: impl ToString) {
        if let Some(project) = self.project.as_mut() {
            project.name = name.to_string();
        } else {
            self.project = Some(Project::new(name.to_string()));
        }
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

    pub fn aqora(&self) -> Result<&AqoraConfig> {
        self.tool
            .as_ref()
            .and_then(|tool| tool.aqora.as_ref())
            .ok_or_else(|| {
                error::user(
                    "No aqora section in pyproject.toml",
                    "Make sure your pyproject.toml has a [tool.aqora] section",
                )
            })
    }

    pub fn toml(&self) -> Result<String> {
        toml::to_string_pretty(self).map_err(|err| {
            error::user(
                &format!("could not serialize pyproject.toml: {}", err),
                "Please make sure your pyproject.toml is valid",
            )
        })
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

    pub fn as_use_case(&self) -> Result<&AqoraUseCaseConfig> {
        match self {
            AqoraConfig::UseCase(use_case) => Ok(use_case),
            AqoraConfig::Submission(_) => Err(error::user(
                "Invalid aqora type",
                "Make sure your pyproject.toml has a [tool.aqora] section \
                    with a 'type' and 'competition' field",
            )),
        }
    }

    pub fn as_submission(&self) -> Result<&AqoraSubmissionConfig> {
        match self {
            AqoraConfig::UseCase(_) => Err(error::user(
                "Invalid aqora type",
                "Make sure your pyproject.toml has a [tool.aqora] section \
                    with a 'type' and 'competition' field",
            )),
            AqoraConfig::Submission(submission) => Ok(submission),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AqoraUseCaseConfig {
    #[serde(with = "crate::id::node_serde")]
    pub competition: Id,
    pub data: PathBuf,
    pub generator: PathStr<'static>,
    pub aggregator: PathStr<'static>,
    pub layers: Vec<LayerConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LayerConfig {
    pub name: String,
    pub transform: Option<PathStr<'static>>,
    pub metric: Option<PathStr<'static>>,
    pub branch: Option<PathStr<'static>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AqoraSubmissionConfig {
    #[serde(with = "crate::id::node_serde")]
    pub competition: Id,
    #[serde(with = "crate::id::node_serde")]
    pub entity: Id,
    pub refs: HashMap<String, PathStr<'static>>,
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
    pub fn replace_refs(&self, refs: &HashMap<String, PathStr>) -> Result<PathStr<'static>> {
        let mut out = Vec::new();
        for part in self.0.iter() {
            if let Some(ref_key) = part.strip_prefix('$') {
                if let Some(replacement) = refs.get(ref_key) {
                    out.extend(replacement.0.iter().cloned());
                } else {
                    return Err(error::user(
                        &format!("No replacement for ${}", ref_key),
                        "Make sure the path is defined in the [tool.aqora.refs] section of \
                        your pyproject.toml",
                    ));
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

pub struct RevertFile {
    backed_up: NamedTempFile,
    path: PathBuf,
    reverted: bool,
}

impl RevertFile {
    pub fn save(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let mut tmp_prefix = OsString::from(".");
        tmp_prefix.push(path.file_name().unwrap_or_else(|| "tmp".as_ref()));
        let backed_up = NamedTempFile::with_prefix_in(
            tmp_prefix,
            path.parent().unwrap_or_else(|| ".".as_ref()),
        )?;
        std::fs::copy(&path, backed_up.path())?;
        Ok(Self {
            backed_up,
            path,
            reverted: false,
        })
    }

    pub fn saved(&self) -> &NamedTempFile {
        &self.backed_up
    }

    pub fn revert(mut self) -> std::io::Result<()> {
        std::fs::copy(self.backed_up.path(), &self.path)?;
        self.reverted = true;
        Ok(())
    }
}

impl AsRef<Path> for RevertFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for RevertFile {
    fn drop(&mut self) {
        if self.reverted {
            return;
        }
        if let Err(err) = std::fs::copy(self.backed_up.path(), &self.path) {
            eprintln!("Could not revert file {}: {}", self.path.display(), err);
        }
    }
}
