use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;

use aqora_data_utils::{infer, parquet, read, write, Schema};
use clap::{Args, ValueEnum};
use futures::prelude::*;
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};

use super::infer::{render_sample_debug, render_schema, FormatOptions, InferOptions, SchemaOutput};
use super::utils::from_json_str_or_file;
use super::GlobalArgs;

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

#[derive(Debug, Serialize, ValueEnum, Clone, Copy, Deserialize)]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstf,
    Lz4Raw,
}

impl From<Compression> for parquet::basic::Compression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => Self::UNCOMPRESSED,
            Compression::Snappy => Self::SNAPPY,
            Compression::Gzip => Self::GZIP(Default::default()),
            Compression::Lzo => Self::LZO,
            Compression::Brotli => Self::BROTLI(Default::default()),
            Compression::Lz4 => Self::LZ4,
            Compression::Zstf => Self::ZSTD(Default::default()),
            Compression::Lz4Raw => Self::LZ4_RAW,
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
    compression: Option<Compression>,
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
    #[arg(value_enum, long, default_value_t = Compression::None)]
    compression: Compression,
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
    metdata: Option<String>,
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
pub struct Convert {
    #[command(flatten)]
    format: Box<FormatOptions>,
    #[command(flatten)]
    infer: Box<InferOptions>,
    #[command(flatten)]
    write: Box<WriteOptions>,
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
    fn parse(&self) -> Result<aqora_data_utils::write::Options> {
        let mut builder = parquet::file::properties::WriterProperties::builder()
            .set_writer_version(self.writer_version.into())
            .set_compression(self.compression.into())
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
        if let Some(metadata) = self.metdata.as_ref() {
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
            let cols: HashMap<Vec<String>, ColumnOptions> = from_json_str_or_file(column_options)?;
            for (name, opts) in cols {
                if let Some(compression) = opts.compression {
                    builder =
                        builder.set_column_compression(name.clone().into(), compression.into());
                }
                if let Some(dictionary_enabled) = opts.dictionary_enabled {
                    builder = builder
                        .set_column_dictionary_enabled(name.clone().into(), dictionary_enabled);
                }
                if let Some(column_statistics_enabled) = opts.column_statistics_enabled {
                    builder = builder.set_column_statistics_enabled(
                        name.clone().into(),
                        column_statistics_enabled.into(),
                    );
                }
                if let Some(bloom_filter_enabled) = opts.bloom_filter_enabled {
                    builder = builder
                        .set_column_bloom_filter_enabled(name.clone().into(), bloom_filter_enabled);
                }
                if let Some(bloom_filter_fpp) = opts.bloom_filter_fpp {
                    builder =
                        builder.set_column_bloom_filter_fpp(name.clone().into(), bloom_filter_fpp);
                }
                if let Some(bloom_filter_ndv) = opts.bloom_filter_ndv {
                    builder =
                        builder.set_column_bloom_filter_ndv(name.clone().into(), bloom_filter_ndv);
                }
            }
        }
        Ok(aqora_data_utils::write::Options::new()
            .with_properties(builder.build())
            .with_schema_root(self.schema_root_name.clone())
            .with_skip_arrow_metadata(self.skip_arrow_metadata))
    }
}

pub async fn convert(args: Convert, global: GlobalArgs) -> Result<()> {
    let output = args.dest.clone().unwrap_or_else(|| {
        let mut name = args
            .src
            .file_stem()
            .unwrap_or("converted".as_ref())
            .to_owned();
        name.push(".parquet");
        name.into()
    });
    let pb = global.spinner().with_message(format!(
        "Converting {} to {}",
        args.src.display(),
        output.display()
    ));
    let mut reader = args.format.open(&args.src).await?;
    let write_options = args.write.parse()?;
    let read_options = read::Options {
        batch_size: Some(args.record_batch_size),
    };
    let stream = if let Some(schema) = args.schema.as_ref() {
        let schema: Schema = from_json_str_or_file(schema)?;
        reader
            .stream_record_batches(schema.clone(), read_options)
            .await
            .map_err(|err| {
                error::user(
                    &format!("Could not read from input file: {err}"),
                    "Please check the file format and try again",
                )
            })?
    } else {
        pb.set_message(format!("Inferring schema of {}", args.src.display()));
        let infer_options = args.infer.parse()?;
        let mut stream = reader.stream_values().await?;
        let sample_size = args.infer.max_samples();
        let mut samples = if let Some(sample_size) = sample_size {
            Vec::with_capacity(sample_size)
        } else {
            Vec::new()
        };
        while let Some(value) = stream.next().await.transpose().map_err(|err| {
            error::user(
                &format!("Failed to read record: {err}"),
                "Check the data or file and try again",
            )
        })? {
            samples.push(value);
            if sample_size.is_some_and(|s| samples.len() >= s) {
                break;
            }
        }
        let schema = if let Ok(schema) = infer::from_samples(&samples, infer_options.clone()) {
            schema
        } else {
            pb.println(render_sample_debug(
                args.schema_output,
                &global,
                infer::debug_samples(&samples, infer_options),
                &samples,
            )?);
            return Err(error::user(
                "Could not infer the schema from the file given",
                "Please make sure the data is conform or set overwrites with --overwrites",
            ));
        };
        read::from_stream(
            futures::stream::iter(samples.into_iter().map(std::io::Result::Ok))
                .chain(stream)
                .boxed_local(),
            schema.clone(),
            read_options,
        )
        .map_err(|err| {
            error::user(
                &format!("Could not read from input file: {err}"),
                "Please check the file format and try again",
            )
        })?
    };

    let schema = stream.schema().clone();
    pb.println(format!(
        "Using schema:\n\n{}\n\n",
        render_schema(args.schema_output, &global, &schema)
            .unwrap_or("Failed to render schema".to_string())
    ));

    let counter = AtomicUsize::new(0);
    let stream = stream.inspect_ok(|batch| {
        counter.fetch_add(batch.num_rows(), std::sync::atomic::Ordering::Relaxed);
        let total_rows = counter.load(std::sync::atomic::Ordering::Relaxed);
        pb.set_message(format!("Wrote {} rows to {}", total_rows, output.display()));
    });

    let writer = tokio::fs::File::create(&output).await.map_err(|err| {
        error::user(
            &format!(
                "Could not open destination file {} for writing: {}",
                output.display(),
                err
            ),
            "Please check the file path and try again",
        )
    })?;

    let metadata = write::from_stream(stream, writer, schema, write_options)
        .await
        .map_err(|err| {
            error::user(
                &format!("An error occurred while writing to the output file: {err}"),
                "Please check the file format and try again",
            )
        })?;

    pb.finish_with_message(format!(
        "Wrote {} rows to {}",
        metadata.num_rows,
        output.display()
    ));
    Ok(())
}
