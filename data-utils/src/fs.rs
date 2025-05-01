use std::io;
use std::path::Path;

use tokio::fs::File;

use crate::format::{FileKind, FormatReader};

impl FileKind {
    pub fn from_ext(ext: impl AsRef<std::ffi::OsStr>) -> Option<Self> {
        match ext.as_ref().to_str()?.to_lowercase().as_str() {
            #[cfg(feature = "csv")]
            "csv" | "tsv" => Some(Self::Csv),
            #[cfg(feature = "json")]
            "json" | "jsonl" => Some(Self::Json),
            _ => None,
        }
    }
}

impl FormatReader<File> {
    pub async fn infer_path(
        path: impl AsRef<Path>,
        max_records: Option<usize>,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let file_kind = path
            .extension()
            .and_then(FileKind::from_ext)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "Extension does not match known formats",
                )
            })?;
        let file = File::open(path).await?;
        FormatReader::infer_format(file, file_kind, max_records).await
    }
}

pub async fn open(path: impl AsRef<Path>) -> io::Result<FormatReader<File>> {
    FormatReader::infer_path(path, Some(100)).await
}
