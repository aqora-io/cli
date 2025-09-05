use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use parquet::arrow::{async_reader::AsyncFileReader, async_writer::AsyncFileWriter};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::InspectReader;
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::async_util::parquet_async::*;
use crate::error::Result;
use crate::read::AsyncFileReaderExt;
use crate::schema::Schema;
use crate::write::{AsyncPartitionWriter, BufferOptions, RecordBatchStreamParquetExt};

use super::io::AsyncBlobReader;
use super::serde::{from_value, to_value};
use super::write::{JsPartWriter, JsWriteOptions};

const COPY_BUFFER_SIZE: usize = 8 * 1024; // same std::sys::io::DEFAULT_BUF_SIZE

#[wasm_bindgen]
pub struct ParquetReader {
    reader: web_sys::Blob,
}

pub struct ProgressAsyncFileReader<R> {
    inner: R,
    progress: Option<js_sys::Function>,
}

impl<R> AsyncFileReader for ProgressAsyncFileReader<R>
where
    R: AsyncFileReader,
{
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        if let Some(progress) = self.progress.as_ref() {
            let _ = progress.call2(progress, &range.start.into(), &range.end.into());
        }
        self.inner.get_bytes(range)
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        if let Some(progress) = self.progress.as_ref() {
            for range in ranges.iter() {
                let _ = progress.call2(progress, &range.start.into(), &range.end.into());
            }
        }
        self.inner.get_byte_ranges(ranges)
    }
}

impl ParquetReader {
    #[inline]
    pub fn async_reader(&self) -> AsyncBlobReader {
        AsyncBlobReader::new(self.reader.clone())
    }

    pub async fn metadata(
        &self,
    ) -> parquet::errors::Result<parquet::arrow::arrow_reader::ArrowReaderMetadata> {
        parquet::arrow::arrow_reader::ArrowReaderMetadata::load_async(
            &mut self.async_reader(),
            Default::default(),
        )
        .await
    }
}

impl ParquetReader {
    async fn parquet_stream<W>(self, mut writer: W, options: JsValue) -> Result<()>
    where
        W: AsyncPartitionWriter,
    {
        let options = from_value::<Option<ParquetWriteOptions>>(options)?.unwrap_or_default();
        if writer
            .max_partition_size()
            .is_none_or(|max_partition_size| self.reader.size() < max_partition_size as f64)
            && options.options.is_default()
        {
            let mut partition = writer.next_partition().await?;
            let mut reader: Box<dyn AsyncRead + Unpin> = Box::new(self.async_reader());
            if let Some(progress) = options.progress {
                let mut offset: usize = 0;
                reader = Box::new(InspectReader::new(reader, move |bytes| {
                    let end = offset + bytes.len();
                    let _ = progress.call2(
                        &progress,
                        &js_sys::BigInt::from(offset),
                        &js_sys::BigInt::from(end),
                    );
                    offset = end;
                }));
            }
            loop {
                let mut buffer = BytesMut::with_capacity(COPY_BUFFER_SIZE);
                if reader.read_buf(&mut buffer).await? == 0 {
                    break;
                }
                partition.write(buffer.freeze()).await?;
            }
            partition.complete().await?;
            return Ok(());
        }
        let metadata = self.metadata().await?;
        let stream =
            parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                self.async_reader()
                    .inspect(move |range| {
                        if let Some(progress) = options.progress.as_ref() {
                            let _ =
                                progress.call2(progress, &range.start.into(), &range.end.into());
                        }
                    })
                    .merge_ranges(),
                metadata,
            )
            .build()?;
        let options = options.options;
        let buffer_options = BufferOptions {
            batch_buffer_size: options.batch_buffer_size,
            row_group_size: options.row_group_size,
            small_first_row_group: options.small_first_row_group,
        };
        stream
            .write_to_parquet(writer, options.try_into()?, buffer_options)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
}

#[wasm_bindgen]
impl ParquetReader {
    #[wasm_bindgen(constructor)]
    pub fn new(reader: web_sys::Blob) -> Self {
        Self { reader }
    }

    #[wasm_bindgen(getter)]
    pub fn reader(&self) -> web_sys::Blob {
        self.reader.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_reader(&mut self, reader: web_sys::Blob) {
        self.reader = reader;
    }

    #[wasm_bindgen(unchecked_return_type = "Schema")]
    pub async fn schema(&self) -> Result<JsValue> {
        Ok(to_value(&Schema::from(Arc::clone(
            self.metadata().await?.schema(),
        )))?)
    }

    #[wasm_bindgen(js_name = "writeParquet")]
    pub async fn write_parquet(
        self,
        writer: &mut JsPartWriter,
        #[wasm_bindgen(unchecked_param_type = "undefined | ParquetWriteOptions | null")]
        options: JsValue,
    ) -> Result<()> {
        self.parquet_stream(writer, options).await
    }

    #[cfg(feature = "aqora-client")]
    #[wasm_bindgen(js_name = "uploadParquet")]
    pub async fn upload_parquet(
        self,
        uploader: &mut super::aqora_client::JsDatasetVersionFileUploader,
        #[wasm_bindgen(unchecked_param_type = "undefined | ParquetWriteOptions | null")]
        options: JsValue,
    ) -> Result<()> {
        self.parquet_stream(uploader, options).await
    }
}

#[derive(TS, Serialize, Deserialize, Default)]
#[ts(export)]
pub struct ParquetWriteOptions {
    #[serde(default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "(start: bigint, end: bigint) => void")]
    pub progress: Option<js_sys::Function>,
    #[serde(flatten)]
    pub options: JsWriteOptions,
}
