use std::path::PathBuf;

use crate::error::{self, Result};
use fs4::tokio::AsyncFileExt;
use futures::future::BoxFuture;
use tokio::fs::File;
use tokio::fs::OpenOptions;

pub async fn with_locked_file<T, F>(f: F, path: PathBuf) -> Result<T>
where
    F: for<'a> FnOnce(&'a mut File) -> BoxFuture<'a, Result<T>> + Send,
    T: Send,
{
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await
        .map_err(|e| {
            error::system(
                &format!("Failed to open file at {}: {:?}", path.display(), e),
                "",
            )
        })?;

    file.lock_exclusive().map_err(|e| {
        error::system(
            &format!("Failed to lock file at {}: {:?}", path.display(), e),
            "",
        )
    })?;

    let res = f(&mut file).await;

    file.unlock().map_err(|e| {
        error::system(
            &format!("Failed to unlock file at {}: {:?}", path.display(), e),
            "",
        )
    })?;

    res
}
