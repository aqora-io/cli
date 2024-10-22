use std::{path::Path, str::FromStr};

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

pub trait PathExt {
    fn archive_kind(&self) -> Option<ArchiveKind>;
}

impl PathExt for Path {
    fn archive_kind(&self) -> Option<ArchiveKind> {
        self.file_name()?.to_str()?.parse().ok()
    }
}
