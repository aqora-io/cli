use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::async_util::parquet_async::*;
use crate::infer::{self};
use crate::process::ProcessItem;
use crate::read::{self, RecordBatchStream};
use crate::schema::Schema;
use crate::value::Value;
use crate::Result;

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

pub(crate) type ValueStream<'a> = BoxStream<'a, io::Result<ProcessItem<Value>>>;

async fn stream_values<'a, R>(mut reader: R, format: &Format) -> io::Result<ValueStream<'a>>
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

pub(crate) async fn take_samples(
    stream: &mut ValueStream<'_>,
    sample_size: Option<usize>,
) -> io::Result<Vec<ProcessItem<Value>>> {
    let mut samples = if let Some(sample_size) = sample_size {
        Vec::with_capacity(sample_size)
    } else {
        Vec::new()
    };
    while let Some(value) = stream.next().await.transpose()? {
        samples.push(value);
        if sample_size.is_some_and(|s| samples.len() >= s) {
            break;
        }
    }
    Ok(samples)
}

impl<R> FormatReader<R>
where
    R: AsyncFileReader,
{
    pub async fn stream_values(&mut self) -> io::Result<ValueStream> {
        stream_values(&mut self.reader, &self.format).await
    }

    pub async fn stream_record_batches(
        &mut self,
        schema: Schema,
        options: read::Options,
    ) -> Result<RecordBatchStream<ValueStream>> {
        read::from_stream(self.stream_values().await?, schema, options)
    }

    pub async fn infer_schema(
        &mut self,
        options: infer::Options,
        sample_size: Option<usize>,
    ) -> Result<Schema> {
        let samples = take_samples(&mut self.stream_values().await?, sample_size).await?;
        Ok(infer::from_samples(&samples, options)?)
    }

    pub async fn infer_and_stream_record_batches(
        &mut self,
        infer_options: infer::Options,
        sample_size: Option<usize>,
        read_options: read::Options,
    ) -> Result<RecordBatchStream<ValueStream>> {
        let mut stream = self.stream_values().await?;
        let samples = take_samples(&mut stream, sample_size).await?;
        let schema = infer::from_samples(&samples, infer_options)?;
        read::from_stream(
            boxed_stream(
                futures::stream::iter(samples.into_iter().map(io::Result::Ok)).chain(stream),
            ),
            schema.clone(),
            read_options,
        )
    }
}

impl<R> FormatReader<R>
where
    R: AsyncFileReader + 'static,
{
    pub async fn into_value_stream(self) -> io::Result<ValueStream<'static>> {
        stream_values(self.reader, &self.format).await
    }

    pub async fn into_record_batch_stream(
        self,
        schema: Schema,
        options: read::Options,
    ) -> Result<RecordBatchStream<ValueStream<'static>>> {
        read::from_stream(self.into_value_stream().await?, schema, options)
    }

    pub async fn into_inferred_record_batch_stream(
        self,
        infer_options: infer::Options,
        sample_size: Option<usize>,
        read_options: read::Options,
    ) -> Result<RecordBatchStream<ValueStream<'static>>> {
        let mut stream = self.into_value_stream().await?;
        let samples = take_samples(&mut stream, sample_size).await?;
        let schema = infer::from_samples(&samples, infer_options)?;
        read::from_stream(
            boxed_stream(
                futures::stream::iter(samples.into_iter().map(io::Result::Ok)).chain(stream),
            ),
            schema.clone(),
            read_options,
        )
    }
}
