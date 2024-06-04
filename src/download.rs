use crate::{
    compress::decompress,
    error::{self, Result},
    progress_bar::{self, TempProgressStyle},
};
use futures::prelude::*;
use indicatif::ProgressBar;
use std::path::Path;
use url::Url;

pub async fn download_tar_gz(url: Url, dir: impl AsRef<Path>, pb: &ProgressBar) -> Result<()> {
    let _guard = TempProgressStyle::new(pb);

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
    let response = client
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
        .map_err(|e| error::system(&format!("Failed to download data: {e}"), ""))?;
    let show_progress = if let Some(content_length) = response.content_length() {
        pb.reset();
        pb.set_style(progress_bar::pretty_bytes());
        pb.disable_steady_tick();
        pb.set_position(0);
        pb.set_length(content_length);
        true
    } else {
        false
    };
    let mut byte_stream = response.bytes_stream();
    let tempfile = tempfile::NamedTempFile::new().map_err(|e| {
        error::user(
            &format!("Failed to create temporary file: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    let mut tar_file = tokio::fs::File::create(tempfile.path()).await?;
    while let Some(item) = byte_stream.next().await {
        let item = item?;
        tokio::io::copy(&mut item.as_ref(), &mut tar_file).await?;
        if show_progress {
            pb.inc(item.len() as u64);
        }
    }
    decompress(tempfile.path(), &dir, pb).await.map_err(|e| {
        error::user(
            &format!("Failed to decompress data: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    Ok(())
}
