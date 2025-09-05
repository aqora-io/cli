use std::collections::HashMap;

use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::io::{SimplexStream, WriteHalf};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::error::{Error, Result};
use crate::write::{AsyncPartitionWriter, BufferOptions, ParquetStream};

use super::error::WasmError;
use super::io::async_read_to_readable_stream;
use super::read::JsRecordBatchStream;
use super::serde::{from_value, to_value};

#[derive(TS, Serialize, Deserialize, Debug, Clone, Default)]
#[ts(export, rename = "StreamWriterOptions")]
pub struct JsPartWriterOptions {
    #[serde(with = "super::serde::preserve")]
    #[ts(type = "(stream: ReadableStream) => void")]
    pub on_stream: js_sys::Function,
    #[ts(optional)]
    pub max_partition_size: Option<usize>,
    #[ts(optional)]
    pub buffer_size: Option<usize>,
}

const DEFAULT_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64 MB

#[wasm_bindgen(js_name = StreamWriter)]
pub struct JsPartWriter {
    on_stream: js_sys::Function,
    max_partition_size: Option<usize>,
    buffer_size: usize,
}

impl JsPartWriter {
    pub fn new(options: JsPartWriterOptions) -> Self {
        Self {
            on_stream: options.on_stream,
            max_partition_size: options.max_partition_size,
            buffer_size: options.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
        }
    }

    fn create_stream(&self) -> Result<WriteHalf<SimplexStream>, WasmError> {
        let (reader, writer) = tokio::io::simplex(self.buffer_size);
        let stream = async_read_to_readable_stream(reader, self.buffer_size);
        self.on_stream.call1(&JsValue::NULL, &stream)?;
        Ok(writer)
    }
}

#[wasm_bindgen(js_class = StreamWriter)]
impl JsPartWriter {
    #[wasm_bindgen(constructor)]
    pub fn js_new(
        #[wasm_bindgen(unchecked_param_type = "StreamWriterOptions")] options: JsValue,
    ) -> Result<Self> {
        Ok(Self::new(from_value(options)?))
    }
}

#[async_trait::async_trait(?Send)]
impl AsyncPartitionWriter for JsPartWriter {
    type Writer = WriteHalf<SimplexStream>;
    async fn next_partition(&mut self) -> std::io::Result<Self::Writer> {
        Ok(self.create_stream()?)
    }
    fn max_partition_size(&self) -> Option<usize> {
        self.max_partition_size
    }
}

#[wasm_bindgen(js_class = "RecordBatchStream")]
impl JsRecordBatchStream {
    async fn parquet_stream<W>(self, writer: W, options: JsValue) -> Result<()>
    where
        W: AsyncPartitionWriter,
    {
        let options = from_value::<Option<RecordBatchWriteOptions>>(options)?.unwrap_or_default();
        let schema = self.0.schema().clone();
        let stream = if let Some(progress) = options.progress {
            self.0
                .map_inner(move |item| {
                    let item = item?;
                    match progress.length() {
                        1 => {
                            progress.call1(&progress, &item.start.into())?;
                        }
                        2 => {
                            progress.call2(&progress, &item.start.into(), &item.end.into())?;
                        }
                        3 => {
                            progress.call3(
                                &progress,
                                &item.start.into(),
                                &item.end.into(),
                                &to_value(&item.item)?,
                            )?;
                        }
                        _ => {
                            return Err(JsError::new(
                                "Expected 1 to 3 arguments for progress callback",
                            )
                            .into())
                        }
                    }
                    Ok::<_, Error>(item)
                })
                .boxed_local()
        } else {
            self.0.boxed_local()
        };
        let options = options.options;
        let buffer_options = BufferOptions {
            batch_buffer_size: options.batch_buffer_size,
            row_group_size: options.row_group_size,
            small_first_row_group: options.small_first_row_group,
        };
        ParquetStream::new(stream, writer, schema, options.try_into()?, buffer_options)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    #[wasm_bindgen(js_name = "writeParquet")]
    pub async fn write_parquet(
        self,
        writer: &mut JsPartWriter,
        #[wasm_bindgen(unchecked_param_type = "undefined | RecordBatchWriteOptions | null")]
        options: JsValue,
    ) -> Result<()> {
        self.parquet_stream(writer, options).await
    }

    #[cfg(feature = "aqora-client")]
    #[wasm_bindgen(js_name = "uploadParquet")]
    pub async fn upload_parquet(
        self,
        uploader: &mut super::aqora_client::JsDatasetVersionFileUploader,
        #[wasm_bindgen(unchecked_param_type = "undefined | RecordBatchWriteOptions | null")]
        options: JsValue,
    ) -> Result<()> {
        self.parquet_stream(uploader, options).await
    }
}

#[derive(TS, Serialize, Deserialize, Default)]
#[ts(export)]
pub struct RecordBatchWriteOptions {
    #[serde(default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "(start: bigint, end: bigint, record: any) => void")]
    pub progress: Option<js_sys::Function>,
    #[serde(flatten)]
    pub options: JsWriteOptions,
}

#[derive(TS, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct JsWriteOptions {
    #[serde(default)]
    #[ts(optional)]
    pub batch_buffer_size: Option<usize>,
    #[serde(default)]
    #[ts(optional)]
    pub row_group_size: Option<usize>,
    #[serde(default)]
    #[ts(optional, as = "Option<bool>")]
    pub small_first_row_group: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub skip_arrow_metadata: Option<bool>,
    #[serde(default)]
    #[ts(optional)]
    pub schema_root: Option<String>,
    #[serde(flatten)]
    pub writer_properties: JsWriterProperties,
}

impl JsWriteOptions {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

impl TryFrom<JsWriteOptions> for parquet::arrow::arrow_writer::ArrowWriterOptions {
    type Error = parquet::errors::ParquetError;
    fn try_from(value: JsWriteOptions) -> Result<Self, Self::Error> {
        let mut options = Self::default().with_properties(value.writer_properties.try_into()?);
        if let Some(skip_arrow_metadata) = value.skip_arrow_metadata {
            options = options.with_skip_arrow_metadata(skip_arrow_metadata);
        }
        if let Some(schema_root) = value.schema_root {
            options = options.with_schema_root(schema_root);
        }
        Ok(options)
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "BloomFilterPosition", export)]
#[serde(rename_all = "snake_case")]
pub enum JsBloomFilterPosition {
    AfterRowGroup,
    End,
}

impl From<JsBloomFilterPosition> for parquet::file::properties::BloomFilterPosition {
    fn from(position: JsBloomFilterPosition) -> Self {
        match position {
            JsBloomFilterPosition::AfterRowGroup => Self::AfterRowGroup,
            JsBloomFilterPosition::End => Self::End,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "WriterVersion", export)]
pub enum JsWriterVersion {
    #[serde(rename = "PARQUET_1_0")]
    Parquet1_0,
    #[serde(rename = "PARQUET_2_0")]
    Parquet2_0,
}

impl From<JsWriterVersion> for parquet::file::properties::WriterVersion {
    fn from(version: JsWriterVersion) -> Self {
        match version {
            JsWriterVersion::Parquet1_0 => Self::PARQUET_1_0,
            JsWriterVersion::Parquet2_0 => Self::PARQUET_2_0,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "Encoding", export)]
#[serde(rename_all = "snake_case")]
pub enum JsEncoding {
    Plain,
    PlainDictionary,
    Rle,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RleDictionary,
    ByteStreamSplit,
}

impl From<JsEncoding> for parquet::basic::Encoding {
    fn from(value: JsEncoding) -> Self {
        match value {
            JsEncoding::Plain => Self::PLAIN,
            JsEncoding::PlainDictionary => Self::PLAIN_DICTIONARY,
            JsEncoding::Rle => Self::RLE,
            JsEncoding::DeltaBinaryPacked => Self::DELTA_BINARY_PACKED,
            JsEncoding::DeltaLengthByteArray => Self::DELTA_LENGTH_BYTE_ARRAY,
            JsEncoding::DeltaByteArray => Self::DELTA_BYTE_ARRAY,
            JsEncoding::RleDictionary => Self::RLE_DICTIONARY,
            JsEncoding::ByteStreamSplit => Self::BYTE_STREAM_SPLIT,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "Compression", export)]
#[serde(tag = "codec", rename_all = "snake_case")]
pub enum JsCompression {
    Uncompressed,
    #[cfg(feature = "snap")]
    Snappy,
    #[cfg(feature = "flate2")]
    Gzip {
        #[serde(default)]
        #[ts(optional)]
        level: Option<u32>,
    },
    #[cfg(feature = "brotli")]
    Brotli {
        #[serde(default)]
        #[ts(optional)]
        level: Option<u32>,
    },
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "zstd")]
    Zstd {
        #[serde(default)]
        #[ts(optional)]
        level: Option<i32>,
    },
    #[cfg(feature = "lz4")]
    Lz4Raw,
}

impl TryFrom<JsCompression> for parquet::basic::Compression {
    type Error = parquet::errors::ParquetError;
    fn try_from(value: JsCompression) -> Result<Self, Self::Error> {
        Ok(match value {
            JsCompression::Uncompressed => Self::UNCOMPRESSED,
            #[cfg(feature = "snap")]
            JsCompression::Snappy => Self::SNAPPY,
            #[cfg(feature = "flate2")]
            JsCompression::Gzip { level } => Self::GZIP(
                level
                    .map(parquet::basic::GzipLevel::try_new)
                    .transpose()?
                    .unwrap_or_default(),
            ),
            #[cfg(feature = "brotli")]
            JsCompression::Brotli { level } => Self::BROTLI(
                level
                    .map(parquet::basic::BrotliLevel::try_new)
                    .transpose()?
                    .unwrap_or_default(),
            ),
            #[cfg(feature = "lz4")]
            JsCompression::Lz4 => Self::LZ4,
            #[cfg(feature = "zstd")]
            JsCompression::Zstd { level } => Self::ZSTD(
                level
                    .map(parquet::basic::ZstdLevel::try_new)
                    .transpose()?
                    .unwrap_or_default(),
            ),
            #[cfg(feature = "lz4")]
            JsCompression::Lz4Raw => Self::LZ4_RAW,
        })
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "EnabledStatistics", export)]
#[serde(rename_all = "snake_case")]
pub enum JsEnabledStatistics {
    None,
    Chunk,
    Page,
}

impl From<JsEnabledStatistics> for parquet::file::properties::EnabledStatistics {
    fn from(value: JsEnabledStatistics) -> Self {
        match value {
            JsEnabledStatistics::None => Self::None,
            JsEnabledStatistics::Chunk => Self::Chunk,
            JsEnabledStatistics::Page => Self::Page,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "SortingColumn", export)]
pub struct JsSortingColumn {
    column_idx: i32,
    descending: bool,
    nulls_first: bool,
}

impl From<JsSortingColumn> for parquet::format::SortingColumn {
    fn from(value: JsSortingColumn) -> Self {
        Self {
            column_idx: value.column_idx,
            descending: value.descending,
            nulls_first: value.nulls_first,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Eq, PartialEq)]
#[ts(rename = "KeyValue", export)]
pub struct JsKeyValue {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    value: Option<String>,
}

impl From<JsKeyValue> for parquet::format::KeyValue {
    fn from(value: JsKeyValue) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Default, Eq, PartialEq)]
#[ts(rename = "ColumnProperties", export)]
pub struct JsColumnProperties {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub encoding: Option<JsEncoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub compression: Option<JsCompression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub dictionary_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub statistics_enabled: Option<JsEnabledStatistics>,
}

#[derive(TS, Serialize, Deserialize, Default, Eq, PartialEq)]
#[ts(rename = "WriterProperties", export)]
pub struct JsWriterProperties {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub data_page_size_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub dictionary_page_size_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub data_page_row_count_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub write_batch_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub max_row_group_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub bloom_filter_position: Option<JsBloomFilterPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub writer_version: Option<JsWriterVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub created_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub offset_index_disabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub key_value_metadata: Option<Vec<JsKeyValue>>,
    #[serde(flatten)]
    pub default_column_properties: JsColumnProperties,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub column_properties: Option<HashMap<String, JsColumnProperties>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub sorting_columns: Option<Vec<JsSortingColumn>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub column_index_truncate_length: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub statistics_truncate_length: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub coerce_types: Option<bool>,
}

impl TryFrom<JsWriterProperties> for parquet::file::properties::WriterProperties {
    type Error = parquet::errors::ParquetError;
    fn try_from(value: JsWriterProperties) -> Result<Self, Self::Error> {
        let mut builder = Self::builder();
        if let Some(data_page_size_limit) = value.data_page_size_limit {
            builder = builder.set_data_page_size_limit(data_page_size_limit);
        }
        if let Some(dictionary_page_size_limit) = value.dictionary_page_size_limit {
            builder = builder.set_dictionary_page_size_limit(dictionary_page_size_limit);
        }
        if let Some(data_page_row_count_limit) = value.data_page_row_count_limit {
            builder = builder.set_data_page_row_count_limit(data_page_row_count_limit);
        }
        if let Some(write_batch_size) = value.write_batch_size {
            builder = builder.set_write_batch_size(write_batch_size);
        }
        if let Some(max_row_group_size) = value.max_row_group_size {
            builder = builder.set_max_row_group_size(max_row_group_size);
        }
        if let Some(bloom_filter_position) = value.bloom_filter_position {
            builder = builder.set_bloom_filter_position(bloom_filter_position.into());
        }
        if let Some(writer_version) = value.writer_version {
            builder = builder.set_writer_version(writer_version.into());
        }
        if let Some(created_by) = value.created_by {
            builder = builder.set_created_by(created_by);
        }
        if let Some(offset_index_disabled) = value.offset_index_disabled {
            builder = builder.set_offset_index_disabled(offset_index_disabled);
        }
        if let Some(key_value_metadata) = value.key_value_metadata {
            builder = builder.set_key_value_metadata(Some(
                key_value_metadata.into_iter().map(Into::into).collect(),
            ));
        }
        if let Some(encoding) = value.default_column_properties.encoding {
            builder = builder.set_encoding(encoding.into());
        }
        if let Some(compression) = value.default_column_properties.compression {
            builder = builder.set_compression(compression.try_into()?);
        }
        if let Some(dictionary_enabled) = value.default_column_properties.dictionary_enabled {
            builder = builder.set_dictionary_enabled(dictionary_enabled);
        }
        if let Some(statistics_enabled) = value.default_column_properties.statistics_enabled {
            builder = builder.set_statistics_enabled(statistics_enabled.into());
        }
        if let Some(column_properties) = value.column_properties {
            for (path, props) in column_properties {
                let path = parquet::schema::types::ColumnPath::new(
                    path.split('.').map(|s| s.to_owned()).collect::<Vec<_>>(),
                );
                if let Some(encoding) = props.encoding {
                    builder = builder.set_column_encoding(path.clone(), encoding.into());
                }
                if let Some(compression) = props.compression {
                    builder = builder.set_compression(compression.try_into()?);
                }
                if let Some(dictionary_enabled) = props.dictionary_enabled {
                    builder =
                        builder.set_column_dictionary_enabled(path.clone(), dictionary_enabled);
                }
                if let Some(statistics_enabled) = props.statistics_enabled {
                    builder = builder
                        .set_column_statistics_enabled(path.clone(), statistics_enabled.into());
                }
            }
        }
        if let Some(sorting_columns) = value.sorting_columns {
            builder = builder
                .set_sorting_columns(Some(sorting_columns.into_iter().map(Into::into).collect()));
        }
        if let Some(column_index_truncate_length) = value.column_index_truncate_length {
            builder = builder.set_column_index_truncate_length(Some(column_index_truncate_length));
        }
        if let Some(statistics_truncate_length) = value.statistics_truncate_length {
            builder = builder.set_statistics_truncate_length(Some(statistics_truncate_length));
        }
        if let Some(coerce_types) = value.coerce_types {
            builder = builder.set_coerce_types(coerce_types);
        }
        Ok(builder.build())
    }
}
