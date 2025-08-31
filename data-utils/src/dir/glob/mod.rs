mod ast;
mod error;
mod parser;
mod regex;
pub(crate) mod utils;
#[cfg(feature = "fs")]
mod walk;

use std::fmt;
use std::path::{Component as PathComponent, Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use parser::is_tag_expr;
use regex::{Glob, Matches};
use utils::path_to_str;

pub use error::GlobError;

#[derive(Default, Debug, Clone)]
pub struct GlobPath {
    root: PathBuf,
    glob: Glob,
}

impl GlobPath {
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.glob.names()
    }

    #[inline]
    pub fn maybe_matches(&self, path: impl AsRef<Path>) -> Result<bool, GlobError> {
        let Ok(short_path) = path.as_ref().strip_prefix(&self.root) else {
            return Ok(false);
        };
        if self.glob.maybe_matches(short_path)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[inline]
    pub fn matches(&self, path: impl AsRef<Path>) -> Result<Option<Matches<'_>>, GlobError> {
        if let Some(mat) = self.glob.matches(path.as_ref().strip_prefix(&self.root)?)? {
            Ok(Some(mat))
        } else {
            Ok(None)
        }
    }
}

impl fmt::Display for GlobPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.root.display())?;
        if !self.glob.is_empty() && self.root.components().next().is_some() {
            f.write_str(std::path::MAIN_SEPARATOR_STR)?;
        }
        write!(f, "{}", self.glob)
    }
}

impl FromStr for GlobPath {
    type Err = GlobError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut components = Path::new(s).components().peekable();
        let mut root = PathBuf::new();
        while let Some(next) = components.peek() {
            if matches!(next, PathComponent::Normal(_)) {
                break;
            }
            root.push(components.next().unwrap());
        }
        while let Some(next) = components.peek() {
            let PathComponent::Normal(component) = next else {
                return Err(GlobError::UnexpectedPathComponent(
                    Path::new(s).to_string_lossy().into(),
                ));
            };
            if is_tag_expr(path_to_str(component.as_ref())?) {
                root.push(components.next().unwrap());
            } else {
                break;
            }
        }
        let rest = components.collect::<PathBuf>();
        let glob = path_to_str(&rest)?.parse()?;
        Ok(GlobPath { root, glob })
    }
}

impl Serialize for GlobPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for GlobPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_display() {
        for str in [
            "",
            "a",
            "/root",
            "/",
            "{name}",
            "../tag/{name}/(a|b)/**/*/a*(b|c|{d})",
        ] {
            assert_eq!(GlobPath::from_str(str).unwrap().to_string(), str);
        }
    }
}
