use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::FileTimes;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use tempfile::NamedTempFile;

type RevertFileMap = HashMap<PathBuf, RevertFile>;

lazy_static::lazy_static! {
    static ref REVERT_FILES: Mutex<RevertFileMap> = std::sync::Mutex::new(HashMap::new());
}

fn acquire_files() -> io::Result<MutexGuard<'static, RevertFileMap>> {
    REVERT_FILES
        .lock()
        .map_err(|_| io::Error::other("Could not lock REVERT_FILES"))
}

pub struct RevertFile {
    backed_up: NamedTempFile,
    file_times: FileTimes,
    path: PathBuf,
    reverted: bool,
}

fn get_filetimes(path: impl AsRef<Path>) -> FileTimes {
    let mut file_times = FileTimes::new();
    if let Ok(metadata) = std::fs::metadata(path.as_ref()) {
        if let Ok(accessed) = metadata.accessed() {
            file_times = file_times.set_accessed(accessed);
        }
        if let Ok(modified) = metadata.modified() {
            file_times = file_times.set_modified(modified);
        }
    }
    file_times
}

impl RevertFile {
    pub fn save(path: impl Into<PathBuf>) -> io::Result<RevertFileHandle> {
        let path = path.into();
        let mut tmp_prefix = OsString::from(".");
        tmp_prefix.push(path.file_name().unwrap_or_else(|| "tmp".as_ref()));
        let backed_up = NamedTempFile::with_prefix_in(
            tmp_prefix,
            path.parent().unwrap_or_else(|| ".".as_ref()),
        )?;
        let file_times = get_filetimes(&path);
        std::fs::copy(&path, backed_up.path())?;
        let mut files = acquire_files()?;
        files.insert(
            path.clone(),
            Self {
                backed_up,
                file_times,
                path: path.clone(),
                reverted: false,
            },
        );
        Ok(RevertFileHandle {
            path,
            reverted: false,
        })
    }

    fn do_revert(&mut self) -> io::Result<()> {
        std::fs::copy(self.backed_up.path(), &self.path)?;
        if let Ok(file) = std::fs::File::open(&self.path) {
            let _ = file.set_times(self.file_times);
        }
        self.reverted = true;
        Ok(())
    }

    pub fn commit(mut self) {
        self.reverted = true;
    }

    pub fn revert(mut self) -> io::Result<()> {
        self.do_revert()?;
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
        if let Err(err) = self.do_revert() {
            tracing::error!("Could not revert file {}: {}", self.path.display(), err);
        }
    }
}

pub struct RevertFileHandle {
    path: PathBuf,
    reverted: bool,
}

impl RevertFileHandle {
    fn remove_file(&self) -> io::Result<RevertFile> {
        let mut files = acquire_files()?;
        let file = files.remove(&self.path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("File {} not found", self.path.display()),
            )
        })?;
        Ok(file)
    }

    fn do_revert(&mut self) -> io::Result<()> {
        self.remove_file()?.revert()?;
        self.reverted = true;
        Ok(())
    }

    pub fn commit(mut self) -> io::Result<()> {
        self.remove_file()?.commit();
        self.reverted = true;
        Ok(())
    }

    pub fn revert(mut self) -> io::Result<()> {
        self.do_revert()?;
        Ok(())
    }
}

impl AsRef<Path> for RevertFileHandle {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for RevertFileHandle {
    fn drop(&mut self) {
        if self.reverted {
            return;
        }
        if let Err(err) = self.do_revert() {
            tracing::error!("Could not revert file {}: {}", self.path.display(), err);
        }
    }
}

pub fn revert_all() -> io::Result<()> {
    let mut files = acquire_files()?;
    let mut files = std::mem::take(&mut *files);
    for (_, file) in files.drain() {
        file.revert()?;
    }
    Ok(())
}
