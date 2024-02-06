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
