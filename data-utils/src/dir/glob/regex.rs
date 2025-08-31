use std::fmt;
use std::path::{Component as PathComponent, Path, MAIN_SEPARATOR as PATH_SEPARATOR};
use std::str::FromStr;

use regex::Regex;

use super::ast::{Expr, Group, Segment};
use super::error::GlobError;
use super::utils::path_to_str;

lazy_static::lazy_static! {
static ref PATH_RE: String = format!(r"\U{:08X}", PATH_SEPARATOR as u32);
static ref NON_PATH_RE: String = format!(r"[^{}]", PATH_RE.as_str());
}

pub type Matches<'a> = Vec<(&'a str, Option<String>)>;

fn normalized_path_str(path: impl AsRef<Path>) -> Result<(usize, String), GlobError> {
    let mut components = path.as_ref().components().peekable();
    if let Some(PathComponent::CurDir) = components.peek() {
        components.next();
    }
    let mut len = 0;
    let mut out = String::new();
    for component in components {
        let PathComponent::Normal(component) = component else {
            return Err(GlobError::UnexpectedPathComponent(
                Path::new(&component).to_string_lossy().into(),
            ));
        };
        out.push(PATH_SEPARATOR);
        out.push_str(path_to_str(component.as_ref())?);
        len += 1;
    }
    Ok((len, out))
}

impl Expr {
    fn names(&self) -> impl Iterator<Item = &str> {
        match self {
            Self::Named(name) => {
                Box::new(std::iter::once(name.as_str())) as Box<dyn Iterator<Item = &str>>
            }
            Self::Either(groups) => Box::new(groups.iter().flat_map(|g| g.names())),
            _ => Box::new(std::iter::empty()),
        }
    }

    fn as_regex(&self) -> String {
        match self {
            Self::Tag(s) => regex::escape(s),
            Self::Named(s) => format!(r"(?<{}>{}+)", regex::escape(s), NON_PATH_RE.as_str()),
            Self::Star => format!("{}*", NON_PATH_RE.as_str()),
            Self::Either(groups) => {
                let re = groups
                    .iter()
                    .map(|g| g.as_regex())
                    .collect::<Vec<_>>()
                    .join("|");
                format!("(?:{re})")
            }
        }
    }
}

impl Group {
    fn names(&self) -> impl Iterator<Item = &str> {
        self.exprs.iter().flat_map(|e| e.names())
    }

    fn as_regex(&self) -> String {
        let mut out = String::new();
        for expr in self.exprs.iter() {
            out.push_str(&expr.as_regex());
        }
        out
    }
}

impl Segment {
    fn names(&self) -> impl Iterator<Item = &str> {
        match self {
            Self::Match(group) => Box::new(group.names()) as Box<dyn Iterator<Item = &str>>,
            _ => Box::new(std::iter::empty()),
        }
    }

    fn as_regex(&self) -> String {
        match self {
            Self::Match(g) => {
                format!("{}{}", PATH_RE.as_str(), g.as_regex())
            }
            Self::DoubleStar => format!("(?:{}{}+)*", PATH_RE.as_str(), NON_PATH_RE.as_str()),
        }
    }
}

#[derive(Clone)]
pub struct Glob {
    segments: Vec<Segment>,
    // names for each segment
    names: Vec<Vec<String>>,
    // can filter directories
    can_filter_dirs: bool,
    // regexes for paths up to each segment
    path_re: Vec<Regex>,
}

impl fmt::Debug for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Glob")
            .field("segments", &self.segments)
            .field("names", &self.names)
            .field("can_filter_dirs", &self.can_filter_dirs)
            .finish()
    }
}

impl fmt::Display for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut segments = self.segments.iter().peekable();
        while let Some(segment) = segments.next() {
            segment.fmt(f)?;
            if segments.peek().is_some() {
                write!(f, "{}", PATH_SEPARATOR)?;
            }
        }
        Ok(())
    }
}

impl Default for Glob {
    fn default() -> Self {
        Glob::new(vec![Segment::DoubleStar]).unwrap()
    }
}

impl Glob {
    fn new(segments: Vec<Segment>) -> Result<Glob, GlobError> {
        let names = segments
            .iter()
            .map(|s| s.names().map(|s| s.to_string()).collect())
            .collect();
        let can_filter_dirs = segments.iter().all(|s| matches!(s, Segment::Match(_)));
        let segments_re = segments.iter().map(|s| s.as_regex()).collect::<Vec<_>>();
        let path_re = (0..=segments.len())
            .map(|len| Regex::new(&format!("^{}$", segments_re[..len].join(""))))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Glob {
            segments,
            names,
            can_filter_dirs,
            path_re,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().flat_map(|s| s.iter().map(|s| s.as_str()))
    }

    pub fn maybe_matches(&self, path: impl AsRef<Path>) -> Result<bool, GlobError> {
        if !self.can_filter_dirs {
            return Ok(true);
        }
        let (len, path_str) = normalized_path_str(path)?;
        let Some(regex) = self.path_re.get(len) else {
            return Ok(false);
        };
        Ok(regex.is_match(&path_str))
    }

    pub fn matches(&self, path: impl AsRef<Path>) -> Result<Option<Matches<'_>>, GlobError> {
        let (_, path_str) = normalized_path_str(path)?;
        let Some(regex) = self.path_re.last() else {
            return Ok(None);
        };
        let Some(captures) = regex.captures(&path_str) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for name in self.names.iter().flatten() {
            out.push((
                name.as_str(),
                captures.name(name).map(|c| c.as_str().to_string()),
            ));
        }
        Ok(Some(out))
    }
}

impl FromStr for Glob {
    type Err = GlobError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Glob::new(
            Path::new(s)
                .components()
                .map(|component| {
                    let PathComponent::Normal(component) = component else {
                        return Err(GlobError::UnexpectedPathComponent(
                            Path::new(s).to_string_lossy().into(),
                        ));
                    };
                    Ok(path_to_str(component.as_ref())?.parse::<Segment>()?)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matches_simple() {
        let glob = Glob::from_str("a").unwrap();
        assert_eq!(glob.matches("a").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("b").unwrap(), None);
        assert_eq!(glob.matches("a/b").unwrap(), None);
        assert!(glob.maybe_matches("a").unwrap());
        assert!(!glob.maybe_matches("b").unwrap());
        assert!(!glob.maybe_matches("a/b").unwrap());
    }

    #[test]
    fn glob_matches_named() {
        let glob = Glob::from_str("{a}").unwrap();
        assert_eq!(
            glob.matches("a").unwrap(),
            Some(vec![("a", Some("a".to_string()))])
        );
        assert_eq!(
            glob.matches("b").unwrap(),
            Some(vec![("a", Some("b".to_string()))])
        );
        assert_eq!(glob.matches("a/b").unwrap(), None);
        assert!(glob.maybe_matches("a").unwrap());
        assert!(glob.maybe_matches("b").unwrap());
        assert!(!glob.maybe_matches("a/b").unwrap());
    }

    #[test]
    fn glob_matches_multiple() {
        let glob = Glob::from_str("a/b").unwrap();
        assert_eq!(glob.matches("a").unwrap(), None);
        assert_eq!(glob.matches("b").unwrap(), None);
        assert_eq!(glob.matches("a/b").unwrap(), Some(vec![]));
        assert!(glob.maybe_matches("a").unwrap());
        assert!(!glob.maybe_matches("b").unwrap());
        assert!(glob.maybe_matches("a/b").unwrap());
        assert!(!glob.maybe_matches("a/b/c").unwrap());
    }

    #[test]
    fn glob_matches_multiple_named() {
        let glob = Glob::from_str("{a}/{b}").unwrap();
        assert_eq!(glob.matches("a").unwrap(), None);
        assert_eq!(glob.matches("b").unwrap(), None);
        assert_eq!(
            glob.matches("a/b").unwrap(),
            Some(vec![
                ("a", Some("a".to_string())),
                ("b", Some("b".to_string())),
            ])
        );
        assert_eq!(
            glob.matches("c/d").unwrap(),
            Some(vec![
                ("a", Some("c".to_string())),
                ("b", Some("d".to_string())),
            ])
        );
        assert_eq!(glob.matches("a/b/c").unwrap(), None);
        assert!(glob.maybe_matches("a").unwrap());
        assert!(glob.maybe_matches("b").unwrap());
        assert!(glob.maybe_matches("a/b").unwrap());
        assert!(!glob.maybe_matches("a/b/c").unwrap());
    }

    #[test]
    fn glob_matches_star() {
        let glob = Glob::from_str("*").unwrap();
        assert_eq!(glob.matches("a").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("b").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("a/b").unwrap(), None);
        assert!(glob.maybe_matches("a").unwrap());
        assert!(glob.maybe_matches("b").unwrap());
        assert!(!glob.maybe_matches("a/b").unwrap());
    }

    #[test]
    fn glob_matches_double_star() {
        let glob = Glob::from_str("**").unwrap();
        assert_eq!(glob.matches("a").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("b").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("a/b").unwrap(), Some(vec![]));
        assert!(glob.maybe_matches("a").unwrap());
        assert!(glob.maybe_matches("b").unwrap());
        assert!(glob.maybe_matches("a/b").unwrap());
    }

    #[test]
    fn glob_matches_either() {
        let glob = Glob::from_str("(a|b)").unwrap();
        assert_eq!(glob.matches("a").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("b").unwrap(), Some(vec![]));
        assert_eq!(glob.matches("c").unwrap(), None);
        assert_eq!(glob.matches("a/b").unwrap(), None);
        assert!(glob.maybe_matches("a").unwrap());
        assert!(glob.maybe_matches("b").unwrap());
        assert!(!glob.maybe_matches("a/b").unwrap());
    }

    #[test]
    fn glob_matches_either_named() {
        let glob = Glob::from_str("(a{a}|b{b})").unwrap();
        assert_eq!(glob.matches("a").unwrap(), None);
        assert_eq!(glob.matches("b").unwrap(), None);
        assert_eq!(
            glob.matches("aA").unwrap(),
            Some(vec![("a", Some("A".to_string())), ("b", None),])
        );
        assert_eq!(
            glob.matches("bB").unwrap(),
            Some(vec![("a", None), ("b", Some("B".to_string())),])
        );
        assert_eq!(glob.matches("cC").unwrap(), None);
        assert_eq!(glob.matches("aA/b").unwrap(), None);
        assert!(!glob.maybe_matches("a").unwrap());
        assert!(!glob.maybe_matches("b").unwrap());
        assert!(glob.maybe_matches("aA").unwrap());
        assert!(glob.maybe_matches("bB").unwrap());
        assert!(!glob.maybe_matches("cC").unwrap());
        assert!(!glob.maybe_matches("aA/b").unwrap());
    }

    #[test]
    fn glob_matches_group() {
        let glob = Glob::from_str("(a|b)*c{d}").unwrap();
        assert_eq!(glob.matches("a").unwrap(), None);
        assert_eq!(glob.matches("b").unwrap(), None);
        assert_eq!(glob.matches("c").unwrap(), None);
        assert_eq!(glob.matches("d").unwrap(), None);
        assert_eq!(
            glob.matches("acd").unwrap(),
            Some(vec![("d", Some("d".to_string()))])
        );
        assert_eq!(
            glob.matches("bcd").unwrap(),
            Some(vec![("d", Some("d".to_string()))])
        );
        assert_eq!(
            glob.matches("axcd").unwrap(),
            Some(vec![("d", Some("d".to_string()))])
        );
        assert!(!glob.maybe_matches("a").unwrap());
        assert!(glob.maybe_matches("acd").unwrap());
        assert!(!glob.maybe_matches("acd/e").unwrap());
    }
}
