use aqora_archiver::{Archiver, Error, Unarchiver};
use indicatif::ProgressBar;
use std::path::Path;

use crate::progress_bar::{self, TempProgressStyle};

pub const DEFAULT_ARCH_EXTENSION: &str = "tar.zst";
pub const DEFAULT_ARCH_MIME_TYPE: &str = "application/zstd";

pub async fn compress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<(), Error> {
    let _pb = TempProgressStyle::new(pb);
    pb.set_style(progress_bar::pretty());
    Archiver::new_with_progress_bar(
        input.as_ref().to_path_buf(),
        output.as_ref().to_path_buf(),
        pb.clone(),
    )
    .asynchronously(tokio::runtime::Handle::current())
    .await
}

pub async fn decompress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<(), Error> {
    let _pb = TempProgressStyle::new(pb);
    pb.set_style(progress_bar::pretty_bytes());
    Unarchiver::new_with_progress_bar(
        input.as_ref().to_path_buf(),
        output.as_ref().to_path_buf(),
        pb.clone(),
    )
    .asynchronously(tokio::runtime::Handle::current())
    .await
}
