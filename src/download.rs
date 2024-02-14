use crate::{
    compress::decompress,
    error::{self, Result},
};
use futures::prelude::*;
use std::path::Path;
use url::Url;

pub async fn download_tar_gz(url: Url, dir: impl AsRef<Path>) -> Result<()> {
    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
        error::user(
            &format!(
                "Failed to create directory {}: {}",
                dir.as_ref().display(),
                e
            ),
            "Please make sure you have permission to create directories in this directory",
        )
    })?;
    let client = reqwest::Client::new();
    let mut byte_stream = client
        .get(url)
        .send()
        .await
        .map_err(|e| {
            error::user(
                &format!("Failed to download data: {e}"),
                "Check your internet connection and try again",
            )
        })?
        .error_for_status()
        .map_err(|e| error::system(&format!("Failed to download data: {e}"), ""))?
        .bytes_stream();
    let tempfile = tempfile::NamedTempFile::new().map_err(|e| {
        error::user(
            &format!("Failed to create temporary file: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    let mut tar_file = tokio::fs::File::create(tempfile.path()).await?;
    while let Some(item) = byte_stream.next().await {
        tokio::io::copy(&mut item?.as_ref(), &mut tar_file).await?;
    }
    decompress(tempfile.path(), &dir).await.map_err(|e| {
        error::user(
            &format!("Failed to decompress data: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    Ok(())
}
