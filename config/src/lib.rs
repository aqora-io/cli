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

#[derive(Error, Debug)]
pub enum VersionError {
    #[error("Project version is missing")]
    MissingVersion,
    #[error("Project version includes pre-release")]
    VersionIncludesPrerelease,
    #[error("Version release contains too many fields")]
    VersionReleaseTooManyFields,
}

impl PyProject {
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

    pub fn version(&self) -> Option<Version> {
        self.project
            .as_ref()
            .and_then(|project| project.version.to_owned())
    }

    pub fn validate_version(&self) -> Result<(), VersionError> {
        if let Some(version) = self.version() {
            if version.any_prerelease() {
                return Err(VersionError::VersionIncludesPrerelease);
            } else if version.release().len() > 3 {
                return Err(VersionError::VersionReleaseTooManyFields);
            }
            Ok(())
        } else {
            Err(VersionError::MissingVersion)
        }
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

pub type RefMap<'a> = HashMap<String, PathStr<'a>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AqoraUseCaseConfig {
    pub competition: Option<String>,
    pub data: PathBuf,
    pub template: Option<PathBuf>,
    pub generator: PathStr<'static>,
    pub aggregator: PathStr<'static>,
    #[serde(default)]
    pub layers: Vec<LayerConfig>,
    #[serde(default)]
    pub tests: HashMap<String, TestConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LayerConfig {
    pub name: String,
    pub transform: Option<FunctionDef>,
    pub context: Option<FunctionDef>,
    pub metric: Option<FunctionDef>,
    pub branch: Option<FunctionDef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LayerOverride {
    pub transform: Option<FunctionDef>,
    pub context: Option<FunctionDef>,
    pub metric: Option<FunctionDef>,
    pub branch: Option<FunctionDef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestConfig {
    #[serde(default)]
    pub refs: RefMap<'static>,
    pub data: Option<PathBuf>,
    pub generator: Option<PathStr<'static>>,
    pub aggregator: Option<PathStr<'static>>,
    #[serde(default)]
    pub overrides: HashMap<String, LayerOverride>,
    pub expected: Option<toml::Value>,
}

#[derive(Error, Debug)]
pub enum TestConfigError {
    #[error("Test not found: {0}")]
    TestNotFound(String),
    #[error("Layer not found: {0}")]
    LayerNotFound(String),
    #[error(transparent)]
    PathStrReplaceError(#[from] PathStrReplaceError),
}

#[derive(Error, Debug)]
pub enum UseCaseConfigValidationError {
    #[error("Generator contains a reference")]
    GeneratorContainsRef,
    #[error("Aggregator contains a reference")]
    AggregatorContainsRef,
}

impl AqoraUseCaseConfig {
    pub fn replace_refs(&mut self, refs: &RefMap) -> Result<(), PathStrReplaceError> {
        self.generator = self.generator.replace_refs(refs)?;
        self.aggregator = self.aggregator.replace_refs(refs)?;
        for layer in self.layers.iter_mut() {
            if let Some(transform) = layer.transform.as_mut() {
                transform.path = transform.path.replace_refs(refs)?;
            }
            if let Some(context) = layer.context.as_mut() {
                context.path = context.path.replace_refs(refs)?;
            }
            if let Some(metric) = layer.metric.as_mut() {
                metric.path = metric.path.replace_refs(refs)?;
            }
            if let Some(branch) = layer.branch.as_mut() {
                branch.path = branch.path.replace_refs(refs)?;
            }
        }
        Ok(())
    }

    pub fn for_test(&self, test_name: &str) -> Result<AqoraUseCaseConfig, TestConfigError> {
        let mut out = self.clone();
        let test = self
            .tests
            .get(test_name)
            .ok_or_else(|| TestConfigError::TestNotFound(test_name.to_string()))?;
        if let Some(data) = test.data.as_ref() {
            out.data = data.clone();
        }
        if let Some(generator) = test.generator.as_ref() {
            out.generator = generator.clone();
        }
        if let Some(aggregator) = test.aggregator.as_ref() {
            out.aggregator = aggregator.clone();
        }
        for (layer_name, override_) in test.overrides.iter() {
            if let Some(layer) = out
                .layers
                .iter_mut()
                .find(|layer| layer.name == *layer_name)
            {
                if let Some(transform) = override_.transform.as_ref() {
                    layer.transform = Some(transform.clone());
                }
                if let Some(context) = override_.context.as_ref() {
                    layer.context = Some(context.clone());
                }
                if let Some(metric) = override_.metric.as_ref() {
                    layer.metric = Some(metric.clone());
                }
                if let Some(branch) = override_.branch.as_ref() {
                    layer.branch = Some(branch.clone());
                }
            } else {
                return Err(TestConfigError::LayerNotFound(layer_name.to_string()));
            }
        }
        out.replace_refs(&test.refs)?;
        Ok(out)
    }

    pub fn validate(&self) -> Result<(), UseCaseConfigValidationError> {
        if self.generator.has_ref() {
            return Err(UseCaseConfigValidationError::GeneratorContainsRef);
        }
        if self.aggregator.has_ref() {
            return Err(UseCaseConfigValidationError::AggregatorContainsRef);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AqoraSubmissionConfig {
    pub competition: Option<String>,
    pub entity: Option<String>,
    #[serde(default)]
    pub refs: RefMap<'static>,
}

#[derive(Clone, Serialize, Debug)]
pub struct FunctionDef {
    pub path: PathStr<'static>,
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
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut path = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "path" => {
                            if path.is_some() {
                                return Err(de::Error::duplicate_field("path"));
                            }
                            path = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(de::Error::unknown_field(key.as_str(), &["path"]));
                        }
                    }
                }
                let path = path.ok_or_else(|| de::Error::missing_field("path"))?;
                Ok(FunctionDef { path })
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
    pub fn has_ref(&self) -> bool {
        self.0.iter().any(|part| part.starts_with('$'))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_str_no_ref() {
        let path_str: PathStr = "foo.bar.baz".parse().unwrap();
        assert_eq!(path_str.module().to_string(), "foo.bar");
        assert_eq!(path_str.name(), "baz");
        assert_eq!(path_str.to_string(), "foo.bar.baz");
        assert!(!path_str.has_ref());
    }

    #[test]
    fn test_path_str_with_ref() {
        let path_str: PathStr = "foo.$bar.baz".parse().unwrap();
        assert_eq!(path_str.module().to_string(), "foo.$bar");
        assert_eq!(path_str.name(), "baz");
        assert_eq!(path_str.to_string(), "foo.$bar.baz");
        assert!(path_str.has_ref());

        let refs: RefMap = vec![("bar".to_string(), "qux.quux".parse().unwrap())]
            .into_iter()
            .collect();
        let replaced = path_str.replace_refs(&refs).unwrap();
        assert_eq!(replaced.module().to_string(), "foo.qux.quux");
        assert_eq!(replaced.name(), "baz");
        assert_eq!(replaced.to_string(), "foo.qux.quux.baz");
        assert!(!replaced.has_ref());
    }
}
