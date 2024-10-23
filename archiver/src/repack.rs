use crate::{error::Result, ArchiveKind, Archiver, Unarchiver};
use std::{io, path::PathBuf};
use tempfile::TempDir;

/// Very naive implementation of a repacker
pub struct Repacker {
    unarchiver: Unarchiver,
    _work: TempDir, // FIXME: avoid temporary directory
    archiver: Archiver,
}

impl Repacker {
    pub fn new(input: PathBuf, output: PathBuf) -> io::Result<Self> {
        let work = TempDir::new()?;
        let unarchiver = Unarchiver::new(input, work.path().to_owned());
        let archiver = Archiver::new(work.path().to_owned(), output);
        Ok(Self {
            _work: work,
            unarchiver,
            archiver,
        })
    }

    pub fn with_source_kind(self, source_kind: ArchiveKind) -> Self {
        Self {
            unarchiver: self.unarchiver.with_source_kind(source_kind),
            ..self
        }
    }

    pub fn without_source_kind(self) -> Self {
        Self {
            unarchiver: self.unarchiver.without_source_kind(),
            ..self
        }
    }

    pub fn with_target_kind(self, target_kind: ArchiveKind) -> Self {
        Self {
            archiver: self.archiver.with_target_kind(target_kind),
            ..self
        }
    }

    pub fn without_target_kind(self) -> Self {
        Self {
            archiver: self.archiver.without_target_kind(),
            ..self
        }
    }

    #[cfg(feature = "indicatif")]
    pub fn with_progress_bar(self, progress_bar: indicatif::ProgressBar) -> Self {
        Self {
            unarchiver: self.unarchiver.with_progress_bar(progress_bar.clone()),
            archiver: self.archiver.with_progress_bar(progress_bar),
            ..self
        }
    }

    #[cfg(feature = "indicatif")]
    pub fn without_progress_bar(self) -> Self {
        Self {
            unarchiver: self.unarchiver.without_progress_bar(),
            archiver: self.archiver.without_progress_bar(),
            ..self
        }
    }

    pub fn synchronously(self) -> Result<()> {
        self.unarchiver.synchronously()?;
        self.archiver.synchronously()?;
        Ok(())
    }

    #[cfg(feature = "tokio")]
    pub async fn asynchronously(self, runtime: tokio::runtime::Handle) -> Result<()> {
        runtime.spawn_blocking(move || self.synchronously()).await?
    }
}
