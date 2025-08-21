use std::path::{Path, PathBuf};

use super::error::GlobError;

#[inline]
pub fn path_buf_to_string(path: PathBuf) -> Result<String, GlobError> {
    path.into_os_string()
        .into_string()
        .map_err(|err| GlobError::InvalidUtf8(err.to_string_lossy().into()))
}

#[inline]
pub fn path_to_str(path: &Path) -> Result<&str, GlobError> {
    path.as_os_str()
        .to_str()
        .ok_or_else(|| GlobError::InvalidUtf8(path.to_string_lossy().into()))
}
