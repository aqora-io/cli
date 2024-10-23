use std::{fmt::Display, path::Path, str::FromStr};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveKind {
    Tar(Option<Compression>),
    Zip,
}

impl FromStr for ArchiveKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(index) = s.rfind(".tar") {
            if index + 5 >= s.len() {
                Ok(Self::Tar(None))
            } else {
                Ok(Self::Tar(Some(s[index + 5..].parse()?)))
            }
        } else if s.ends_with(".zip") {
            Ok(Self::Zip)
        } else {
            Err(())
        }
    }
}

impl Default for ArchiveKind {
    fn default() -> Self {
        Self::Tar(Some(Compression::default()))
    }
}

impl Display for ArchiveKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zip => f.write_str("zip"),
            Self::Tar(Some(compression)) => f.write_fmt(format_args!("tar.{compression}")),
            Self::Tar(None) => f.write_str("tar"),
        }
    }
}

#[derive(Default, Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    Gzip,
    #[default]
    Zstandard,
}

impl FromStr for Compression {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gz" => Ok(Compression::Gzip),
            "zst" => Ok(Compression::Zstandard),
            _ => Err(()),
        }
    }
}

impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Gzip => f.write_str("gz"),
            Self::Zstandard => f.write_str("zst"),
        }
    }
}

pub trait PathExt {
    fn archive_kind(&self) -> Option<ArchiveKind>;
}

impl PathExt for Path {
    fn archive_kind(&self) -> Option<ArchiveKind> {
        self.file_name()?.to_str()?.parse().ok()
    }
}
