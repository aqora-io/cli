use futures::{prelude::*, stream::LocalBoxStream};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::infer::{self, Schema};
use crate::read::{self, RecordBatchStream};
use crate::value::Value;
use crate::Result;

pub trait AsyncFileReader: AsyncRead + AsyncSeek + Unpin {}

impl<T> AsyncFileReader for T where T: AsyncRead + AsyncSeek + Unpin {}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Format {
    #[cfg(feature = "csv")]
    Csv(crate::csv::CsvFormat),
    #[cfg(feature = "json")]
    Json(crate::json::JsonFormat),
}

pub struct FormatReader<R> {
    reader: R,
    format: Format,
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

type ValueStream<'a> = LocalBoxStream<'a, io::Result<Value>>;

impl<R> FormatReader<R>
where
    R: AsyncFileReader,
{
    pub async fn stream_values(&mut self) -> io::Result<ValueStream> {
        self.reader.rewind().await?;
        Ok(match &self.format {
            #[cfg(feature = "json")]
            Format::Json(format) => crate::json::read(&mut self.reader, format.clone())
                .await?
                .boxed_local(),
            #[cfg(feature = "csv")]
            Format::Csv(format) => crate::csv::read(&mut self.reader, format.clone())
                .await?
                .boxed_local(),
        })
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
        let mut stream = self.stream_values().await?.boxed_local();
        if let Some(sample_size) = sample_size {
            stream = stream.take(sample_size).boxed_local()
        }
        let values = stream.try_collect::<Vec<_>>().await?;
        Ok(infer::from_samples(&values, options)?)
    }

    pub async fn infer_and_stream_record_batches(
        &mut self,
        infer_options: infer::Options,
        sample_size: Option<usize>,
        read_options: read::Options,
    ) -> Result<RecordBatchStream<ValueStream>> {
        let mut stream = self.stream_values().await?;
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
        let schema = infer::from_samples(&samples, infer_options)?;
        read::from_stream(
            futures::stream::iter(samples.into_iter().map(io::Result::Ok))
                .chain(stream)
                .boxed_local(),
            schema.clone(),
            read_options,
        )
    }
}
