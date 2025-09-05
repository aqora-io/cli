use std::collections::HashMap;
use std::path::{Path, PathBuf};

use aqora_data_utils::{fs::DirWriter, parquet, read, write};
use clap::{Args, ValueEnum};
use futures::prelude::*;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};

use crate::commands::GlobalArgs;
use crate::error::{self, Result};

use super::infer::{open, FormatOptions, InferOptions, OpenOptions, SchemaOutput};
use super::utils::from_json_str_or_file;

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum WriterVersion {
    Parquet1_0,
    Parquet2_0,
}

impl From<WriterVersion> for parquet::file::properties::WriterVersion {
    fn from(value: WriterVersion) -> Self {
        match value {
            WriterVersion::Parquet1_0 => Self::PARQUET_1_0,
            WriterVersion::Parquet2_0 => Self::PARQUET_2_0,
        }
    }
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum BloomFilterPosition {
    AfterRowGroup,
    End,
}

impl From<BloomFilterPosition> for parquet::file::properties::BloomFilterPosition {
    fn from(value: BloomFilterPosition) -> Self {
        match value {
            BloomFilterPosition::AfterRowGroup => Self::AfterRowGroup,
            BloomFilterPosition::End => Self::End,
        }
    }
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy, Deserialize)]
pub enum Encoding {
    Plain,
    PlainDictionary,
    Rle,
    BitPacked,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RleDictionary,
    ByteStreamSplit,
}

impl From<Encoding> for parquet::basic::Encoding {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::Plain => Self::PLAIN,
            Encoding::PlainDictionary => Self::PLAIN_DICTIONARY,
            Encoding::Rle => Self::RLE,
            #[allow(deprecated)]
            Encoding::BitPacked => Self::BIT_PACKED,
            Encoding::DeltaBinaryPacked => Self::DELTA_BINARY_PACKED,
            Encoding::DeltaLengthByteArray => Self::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DeltaByteArray => Self::DELTA_BYTE_ARRAY,
            Encoding::RleDictionary => Self::RLE_DICTIONARY,
            Encoding::ByteStreamSplit => Self::BYTE_STREAM_SPLIT,
        }
    }
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy, Deserialize)]
pub enum EnabledStatistics {
    None,
    Chunk,
    Page,
}

impl From<EnabledStatistics> for parquet::file::properties::EnabledStatistics {
    fn from(value: EnabledStatistics) -> Self {
        match value {
            EnabledStatistics::None => Self::None,
            EnabledStatistics::Chunk => Self::Chunk,
            EnabledStatistics::Page => Self::Page,
        }
    }
}

#[derive(Deserialize)]
pub struct SortingColumn {
    pub column_idx: i32,
    pub descending: bool,
    pub nulls_first: bool,
}

impl From<SortingColumn> for parquet::format::SortingColumn {
    fn from(value: SortingColumn) -> Self {
        parquet::format::SortingColumn {
            column_idx: value.column_idx,
            descending: value.descending,
            nulls_first: value.nulls_first,
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum ColumnPath {
    String(String),
    Pargs(Vec<String>),
}

impl From<ColumnPath> for parquet::schema::types::ColumnPath {
    fn from(value: ColumnPath) -> Self {
        match value {
            ColumnPath::String(s) => Self::from(s),
            ColumnPath::Pargs(p) => Self::from(p),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ColumnOptions {
    compression: Option<String>,
    dictionary_enabled: Option<bool>,
    column_statistics_enabled: Option<EnabledStatistics>,
    bloom_filter_enabled: Option<bool>,
    bloom_filter_fpp: Option<f64>,
    bloom_filter_ndv: Option<u64>,
}

#[derive(Args, Debug, Serialize)]
pub struct WriteOptions {
    #[arg(long, default_value = "arrow_schema")]
    schema_root_name: String,
    #[arg(long)]
    skip_arrow_metadata: bool,
    #[arg(value_enum, long, default_value_t = WriterVersion::Parquet1_0)]
    writer_version: WriterVersion,
    #[arg(value_enum, long, default_value = "zstd(8)")]
    compression: String,
    #[arg(value_enum, long)]
    encoding: Option<Encoding>,
    #[arg(long, default_value_t = 1024 * 1024)]
    data_page_size: usize,
    #[arg(long, default_value_t = 20_000)]
    data_page_row_count: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    dictionary_page_size: usize,
    #[arg(long, default_value_t = 1024)]
    write_batch_size: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    max_row_group_size: usize,
    #[arg(value_enum, long, default_value_t = BloomFilterPosition::AfterRowGroup)]
    bloom_filter_position: BloomFilterPosition,
    #[arg(long)]
    created_by: Option<String>,
    #[arg(long)]
    offset_index_disabled: bool,
    #[arg(long)]
    metadata: Option<String>,
    #[arg(long)]
    sorting_columns: Option<String>,
    #[arg(long)]
    no_dictionary_enabled: bool,
    #[arg(value_enum, long, default_value_t = EnabledStatistics::Page)]
    statistics_enabled: EnabledStatistics,
    #[arg(long)]
    bloom_filter_enabled: bool,
    #[arg(long, default_value_t = 0.05)]
    bloom_filter_fpp: f64,
    #[arg(long, default_value_t = 1_000_000)]
    bloom_filter_ndv: u64,
    #[arg(long)]
    statistics_truncate_length: Option<usize>,
    #[arg(long)]
    column_options: Option<String>,
    #[arg(long)]
    column_index_truncate_length: Option<usize>,
    #[arg(long)]
    coerce_types: bool,
}

#[derive(Args, Debug, Serialize)]
pub struct WriterOptions {
    #[arg(long, default_value_t = 1_000_000_000)]
    max_part_size: usize,
    #[arg(long, default_value = "{part:03}.parquet")]
    template: String,
    #[arg(long)]
    no_try_single: bool,
}

#[derive(Args, Debug, Serialize)]
pub struct BufferOptions {
    #[arg(long, default_value_t = 100)]
    batch_buffer_size: usize,
    #[arg(long, default_value_t = 100 * 1024 * 1024)]
    row_group_size: usize,
    #[arg(long)]
    no_small_first_row_group: bool,
}

impl BufferOptions {
    pub fn parse(&self) -> write::BufferOptions {
        write::BufferOptions {
            batch_buffer_size: if self.batch_buffer_size > 0 {
                Some(self.batch_buffer_size)
            } else {
                None
            },
            row_group_size: if self.row_group_size > 0 {
                Some(self.row_group_size)
            } else {
                None
            },
            small_first_row_group: !self.no_small_first_row_group,
        }
    }
}

#[derive(Args, Debug, Serialize)]
pub struct Convert {
    #[command(flatten)]
    format: Box<FormatOptions>,
    #[command(flatten)]
    infer: Box<InferOptions>,
    #[command(flatten)]
    write: Box<WriteOptions>,
    #[command(flatten)]
    writer: Box<WriterOptions>,
    #[command(flatten)]
    buffer: Box<BufferOptions>,
    #[arg(long)]
    schema: Option<String>,
    #[arg(long, default_value_t = 1024)]
    record_batch_size: usize,
    #[arg(long, value_enum, default_value_t = SchemaOutput::Table)]
    schema_output: SchemaOutput,
    src: PathBuf,
    dest: Option<PathBuf>,
}

impl WriteOptions {
    pub(crate) fn parse(&self) -> Result<aqora_data_utils::write::Options> {
        let mut builder = parquet::file::properties::WriterProperties::builder()
            .set_writer_version(self.writer_version.into())
            .set_compression(self.compression.parse().map_err(|err| {
                error::user(
                    &format!("Invalid compression type: {err}"),
                    "Please check the compression type and try again",
                )
            })?)
            .set_data_page_size_limit(self.data_page_size)
            .set_data_page_row_count_limit(self.data_page_row_count)
            .set_dictionary_page_size_limit(self.dictionary_page_size)
            .set_write_batch_size(self.write_batch_size)
            .set_max_row_group_size(self.max_row_group_size)
            .set_bloom_filter_position(self.bloom_filter_position.into())
            .set_offset_index_disabled(self.offset_index_disabled)
            .set_dictionary_enabled(!self.no_dictionary_enabled)
            .set_statistics_enabled(self.statistics_enabled.into())
            .set_bloom_filter_enabled(self.bloom_filter_enabled)
            .set_bloom_filter_fpp(self.bloom_filter_fpp)
            .set_bloom_filter_ndv(self.bloom_filter_ndv)
            .set_column_index_truncate_length(self.column_index_truncate_length)
            .set_statistics_truncate_length(self.statistics_truncate_length)
            .set_coerce_types(self.coerce_types);
        if let Some(encoding) = self.encoding {
            builder = builder.set_encoding(encoding.into());
        }
        if let Some(created_by) = self.created_by.as_ref() {
            builder = builder.set_created_by(created_by.into());
        }
        if let Some(metadata) = self.metadata.as_ref() {
            let metadata: HashMap<String, String> = from_json_str_or_file(metadata)?;
            builder = builder.set_key_value_metadata(Some(
                metadata
                    .into_iter()
                    .map(|(k, v)| parquet::file::metadata::KeyValue {
                        key: k,
                        value: Some(v),
                    })
                    .collect(),
            ));
        }
        if let Some(sorting_columns) = self.sorting_columns.as_ref() {
            let cols: Vec<SortingColumn> = from_json_str_or_file(sorting_columns)?;
            builder =
                builder.set_sorting_columns(Some(cols.into_iter().map(|c| c.into()).collect()));
        }
        if let Some(column_options) = self.column_options.as_ref() {
            let cols: HashMap<String, ColumnOptions> = from_json_str_or_file(column_options)?;
            for (name, opts) in cols {
                let name = parquet::schema::types::ColumnPath::new(
                    name.split(".").map(|s| s.to_string()).collect::<Vec<_>>(),
                );
                if let Some(compression) = opts.compression {
                    builder = builder.set_column_compression(
                        name.clone(),
                        compression.parse().map_err(|err| {
                            error::user(
                                &format!("Invalid compression type: {err}"),
                                "Please check the compression type and try again",
                            )
                        })?,
                    )
                }
                if let Some(dictionary_enabled) = opts.dictionary_enabled {
                    builder =
                        builder.set_column_dictionary_enabled(name.clone(), dictionary_enabled);
                }
                if let Some(column_statistics_enabled) = opts.column_statistics_enabled {
                    builder = builder.set_column_statistics_enabled(
                        name.clone(),
                        column_statistics_enabled.into(),
                    );
                }
                if let Some(bloom_filter_enabled) = opts.bloom_filter_enabled {
                    builder =
                        builder.set_column_bloom_filter_enabled(name.clone(), bloom_filter_enabled);
                }
                if let Some(bloom_filter_fpp) = opts.bloom_filter_fpp {
                    builder = builder.set_column_bloom_filter_fpp(name.clone(), bloom_filter_fpp);
                }
                if let Some(bloom_filter_ndv) = opts.bloom_filter_ndv {
                    builder = builder.set_column_bloom_filter_ndv(name.clone(), bloom_filter_ndv);
                }
            }
        }
        Ok(aqora_data_utils::write::Options::new()
            .with_properties(builder.build())
            .with_schema_root(self.schema_root_name.clone())
            .with_skip_arrow_metadata(self.skip_arrow_metadata))
    }
}

pub struct WrappedDirWriter {
    writer: DirWriter,
    pb: ProgressBar,
}

#[async_trait::async_trait]
impl write::AsyncPartitionWriter for WrappedDirWriter {
    type Writer = <DirWriter as write::AsyncPartitionWriter>::Writer;

    async fn next_partition(&mut self) -> std::io::Result<Self::Writer> {
        let path = self.writer.next_path();
        let file = self.writer.next_partition().await?;
        self.pb
            .set_message(format!("Writing to {}", path.display()));
        Ok(file)
    }

    fn max_partition_size(&self) -> Option<usize> {
        self.writer.max_partition_size()
    }
}

impl WrappedDirWriter {
    pub fn new(
        input: impl AsRef<Path>,
        output: Option<impl AsRef<Path>>,
        options: WriterOptions,
        pb: ProgressBar,
    ) -> Self {
        let mut writer = if let Some(path) = output.as_ref() {
            let mut writer = DirWriter::new(path);
            if !options.no_try_single {
                writer = writer.with_try_single(path);
            }
            writer
        } else {
            let name = input.as_ref().file_stem().unwrap_or("converted".as_ref());
            let mut writer = DirWriter::new(name);
            if !options.no_try_single {
                let mut single_name = name.to_owned();
                single_name.push(".parquet");
                writer = writer.with_try_single(single_name);
            }
            writer
        };
        writer = writer
            .with_max_part_size(options.max_part_size)
            .with_template(options.template);
        WrappedDirWriter { writer, pb }
    }
}

pub async fn convert(args: Convert, global: GlobalArgs) -> Result<()> {
    let pb = global
        .spinner()
        .with_message(format!("Reading {}", args.src.display()));

    let write_options = args.write.parse()?;
    let (schema, stream) = open(
        &args.src,
        OpenOptions {
            format: *args.format,
            infer: *args.infer,
            read: read::Options {
                batch_size: Some(args.record_batch_size),
            },
            progress: Some(pb.clone()),
            schema: None,
        },
        &global,
        args.schema_output,
    )
    .await?;

    pb.println("\n");
    pb.set_message("Writing...");
    pb.set_style(crate::progress_bar::pretty_bytes());

    let writer = WrappedDirWriter::new(&args.src, args.dest.as_ref(), *args.writer, pb.clone());
    let metadata =
        write::ParquetStream::new(stream, writer, schema, write_options, args.buffer.parse())
            .try_collect::<Vec<_>>()
            .await
            .map_err(|err| {
                error::user(
                    &format!("An error occurred while writing to the output file: {err}"),
                    "Please check the file format and try again",
                )
            })?;

    pb.set_style(indicatif::ProgressStyle::default_spinner());
    pb.finish_with_message(format!(
        "{} records written",
        metadata.iter().map(|(_, meta)| meta.num_rows).sum::<i64>(),
    ));
    Ok(())
}
