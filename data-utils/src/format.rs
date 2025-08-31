use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::async_util::parquet_async::*;
use crate::process::ProcessItemStream;

pub trait AsyncFileReader: AsyncRead + AsyncSeek + MaybeSend + Unpin {}

impl<T> AsyncFileReader for T where T: AsyncRead + AsyncSeek + MaybeSend + Unpin {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FileKind {
    #[cfg(feature = "csv")]
    Csv,
    #[cfg(feature = "json")]
    Json,
}

impl FileKind {
    pub fn from_ext(ext: impl AsRef<std::ffi::OsStr>) -> Option<Self> {
        Some(match ext.as_ref().to_str()?.to_lowercase().as_str() {
            #[cfg(feature = "csv")]
            "csv" | "tsv" => Self::Csv,
            #[cfg(feature = "json")]
            "json" | "jsonl" => Self::Json,
            _ => return None,
        })
    }

    pub fn from_mime(mime: impl AsRef<str>) -> Option<Self> {
        let ty = mime
            .as_ref()
            .split(';')
            .next()
            .unwrap_or_else(|| mime.as_ref())
            .trim()
            .to_lowercase();
        Some(match ty.as_str() {
            #[cfg(feature = "csv")]
            "text/csv" => Self::Csv,
            #[cfg(feature = "json")]
            "application/json" => Self::Json,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Format {
    #[cfg(feature = "csv")]
    Csv(crate::csv::CsvFormat),
    #[cfg(feature = "json")]
    Json(crate::json::JsonFormat),
}

impl Format {
    pub fn file_kind(&self) -> FileKind {
        match self {
            #[cfg(feature = "csv")]
            Self::Csv(_) => FileKind::Csv,
            #[cfg(feature = "json")]
            Self::Json(_) => FileKind::Json,
        }
    }
}

pub struct FormatReader<R> {
    pub(crate) reader: R,
    pub(crate) format: Format,
}

impl<R> FormatReader<R> {
    pub fn new(reader: R, format: Format) -> Self {
        Self { reader, format }
    }

    pub fn format(&self) -> &Format {
        &self.format
    }

    pub fn format_mut(&mut self) -> &mut Format {
        &mut self.format
    }

    pub fn reader(&self) -> &R {
        &self.reader
    }

    pub fn reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    pub fn into_reader(self) -> R {
        self.reader
    }
}

impl<R> FormatReader<R>
where
    R: AsyncFileReader,
{
    pub async fn infer_format(
        mut reader: R,
        kind: FileKind,
        #[allow(dead_code)] max_records: Option<usize>,
    ) -> io::Result<FormatReader<R>> {
        match kind {
            #[cfg(feature = "csv")]
            FileKind::Csv => {
                let format = crate::csv::infer_format(&mut reader, max_records).await?;
                reader.rewind().await?;
                Ok(Self::new(reader, Format::Csv(format)))
            }
            #[cfg(feature = "json")]
            FileKind::Json => {
                let format = crate::json::infer_format(&mut reader).await?;
                reader.rewind().await?;
                Ok(Self::new(reader, Format::Json(format)))
            }
        }
    }
}

async fn stream_values<'a, R>(mut reader: R, format: &Format) -> io::Result<ProcessItemStream<'a>>
where
    R: AsyncFileReader + 'a,
{
    reader.rewind().await?;
    Ok(match format {
        #[cfg(feature = "json")]
        Format::Json(format) => boxed_stream(crate::json::read(reader, format.clone()).await?),
        #[cfg(feature = "csv")]
        Format::Csv(format) => boxed_stream(crate::csv::read(reader, format.clone()).await?),
    })
}

impl<R> FormatReader<R>
where
    R: AsyncFileReader,
{
    pub async fn stream_values(&mut self) -> io::Result<ProcessItemStream<'_>> {
        stream_values(&mut self.reader, &self.format).await
    }
}

impl<R> FormatReader<R>
where
    R: AsyncFileReader + 'static,
{
    pub async fn into_value_stream(self) -> io::Result<ProcessItemStream<'static>> {
        stream_values(self.reader, &self.format).await
    }
}
