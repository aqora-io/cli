use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aqora_data_utils::{
    arrow::record_batch::RecordBatch,
    csv::{infer_format as infer_csv_format, CsvFormat},
    dir::DirReaderOptions,
    infer::{self, SampleDebug},
    json::{infer_format as infer_json_format, JsonFileOptions, JsonFormat, JsonItemOptions},
    read::{self, AsyncFileReaderExt, RecordBatchStreamExt, ValueStream},
    schema::Schema,
    utils::is_parquet,
    value::Value,
    DateParseOptions, Format, FormatReader, ProcessItem,
};
use clap::{Args, ValueEnum};
use futures::{
    prelude::*,
    stream::{BoxStream, FuturesUnordered},
};
use indicatif::ProgressBar;
use regex::Regex;
use serde::Serialize;
use tokio::io::AsyncSeekExt;

use crate::commands::GlobalArgs;
use crate::error::{self, Result};

use super::utils::{fields_into_table, from_json_str_or_file};

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum SchemaOutput {
    Table,
    Json,
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum FileTypeArg {
    Json,
    Csv,
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum JsonItemTypeArg {
    Object,
    List,
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum JsonFileTypeArg {
    Json,
    Jsonl,
}

#[derive(Args, Debug, Serialize)]
pub struct FormatOptions {
    #[arg(value_enum, long)]
    file_type: Option<FileTypeArg>,
    #[arg(value_enum, long)]
    json_item_type: Option<JsonItemTypeArg>,
    #[arg(value_enum, long)]
    json_file_type: Option<JsonFileTypeArg>,
    #[arg(long)]
    json_key_col: Option<String>,
    #[arg(long)]
    has_headers: Option<bool>,
    #[arg(long)]
    date_fmt: Option<String>,
    #[arg(long)]
    timestamp_fmt: Option<String>,
    #[arg(long)]
    csv_delimiter: Option<char>,
    #[arg(long)]
    csv_terminator: Option<char>,
    #[arg(long)]
    csv_quote: Option<char>,
    #[arg(long)]
    csv_escape: Option<char>,
    #[arg(long)]
    csv_comment: Option<char>,
    #[arg(long)]
    csv_null_regex: Option<String>,
    #[arg(long)]
    csv_true_regex: Option<String>,
    #[arg(long)]
    csv_false_regex: Option<String>,
}

#[derive(Args, Debug, Serialize)]
pub struct InferOptions {
    #[arg(long, default_value_t = 1000)]
    max_samples: usize,
    #[arg(long)]
    forbid_null_fields: bool,
    #[arg(long)]
    no_map_as_struct: bool,
    #[arg(long)]
    small_list: bool,
    #[arg(long)]
    small_string: bool,
    #[arg(long)]
    small_bytes: bool,
    #[arg(long)]
    string_dict_encoding: bool,
    #[arg(long)]
    no_coerce_numbers: bool,
    #[arg(long)]
    forbid_to_string: bool,
    #[arg(long)]
    no_guess_dates: bool,
    #[arg(long, default_value_t = 100)]
    from_type_budget: usize,
    #[arg(long)]
    no_unit_enum_as_string: bool,
    #[arg(long)]
    overwrite: Option<String>,
}

#[derive(Args, Debug, Serialize)]
pub struct Infer {
    #[command(flatten)]
    format: Box<FormatOptions>,
    #[command(flatten)]
    infer: Box<InferOptions>,
    #[arg(value_enum, long, default_value_t = SchemaOutput::Table)]
    output: SchemaOutput,
    file: PathBuf,
}

impl InferOptions {
    pub fn max_samples(&self) -> Option<usize> {
        if self.max_samples > 0 {
            Some(self.max_samples)
        } else {
            None
        }
    }
    pub fn parse(&self) -> Result<infer::Options> {
        let mut options = infer::Options {
            forbid_null_fields: self.forbid_null_fields,
            no_coerce_numbers: self.no_coerce_numbers,
            forbid_to_string: self.forbid_to_string,
            no_map_as_struct: self.no_map_as_struct,
            sequence_as_small_list: self.small_list,
            string_as_small_utf8: self.small_string,
            bytes_as_small_binary: self.small_bytes,
            no_unit_enum_as_string: self.no_unit_enum_as_string,
            string_dictionary_encoding: self.string_dict_encoding,
            no_guess_dates: self.no_guess_dates,
            from_type_budget: self.from_type_budget,
            overwrites: Default::default(),
        };
        if let Some(overwrites) = self.overwrite.as_ref() {
            let overwrites: HashMap<String, aqora_data_utils::schema::SerdeField> =
                from_json_str_or_file(overwrites)?;
            for (name, value) in overwrites {
                options.overwrite(name, value);
            }
        }
        Ok(options)
    }
}

impl FormatOptions {
    pub async fn open(
        &self,
        path: &Path,
        mut file: tokio::fs::File,
    ) -> Result<FormatReader<tokio::fs::File>> {
        let file_type = if let Some(file_type) = self.file_type {
            file_type
        } else {
            match path
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.to_lowercase())
                .as_deref()
            {
                Some("json") | Some("jsonl") => FileTypeArg::Json,
                Some("csv") | Some("tsv") => FileTypeArg::Csv,
                _ => {
                    return Err(error::user(
                        "Could not determine file type",
                        "Please select a file type with --file-type",
                    ))
                }
            }
        };
        let date_parse = DateParseOptions {
            date_fmt: self.date_fmt.clone(),
            timestamp_fmt: self.timestamp_fmt.clone(),
        };
        let format = match file_type {
            FileTypeArg::Json => {
                let mut format = infer_json_format(&mut file)
                    .await
                    .unwrap_or_else(|_| JsonFormat::default());
                file.rewind().await.map_err(|e| {
                    error::user(
                        &format!("Could not rewind file: {}", path.display()),
                        &format!("Error: {}", e),
                    )
                })?;
                if let Some(file_type) = self.json_file_type {
                    format.file = match file_type {
                        JsonFileTypeArg::Json => JsonFileOptions::Json {
                            key_col: self
                                .json_key_col
                                .as_deref()
                                .or_else(|| format.file.key_col())
                                .map(|s| s.to_string()),
                        },
                        JsonFileTypeArg::Jsonl => JsonFileOptions::Jsonl,
                    };
                }
                if let Some(item_type) = self.json_item_type {
                    format.item = match item_type {
                        JsonItemTypeArg::Object => JsonItemOptions::Object,
                        JsonItemTypeArg::List => JsonItemOptions::List {
                            has_headers: format.item.has_headers(),
                        },
                    }
                }
                if let Some(headers) = self.has_headers {
                    format.item.set_has_headers(headers)
                }
                format.date = date_parse;
                Format::Json(format)
            }
            FileTypeArg::Csv => {
                let mut format =
                    infer_csv_format(&mut file, Some(100))
                        .await
                        .unwrap_or_else(|err| {
                            tracing::warn!("{err}");
                            CsvFormat::default()
                        });
                file.rewind().await.map_err(|e| {
                    error::user(
                        &format!("Could not rewind file: {}", path.display()),
                        &format!("Error: {}", e),
                    )
                })?;

                if let Some(headers) = self.has_headers {
                    format.has_headers = headers
                }
                if let Some(delimiter) = self.csv_delimiter {
                    format.chars.delimiter = delimiter.try_into().map_err(|err| {
                        error::user(
                            &format!("Invalid CSV delimiter: {err}"),
                            "Please provide a valid CSV delimiter with --csv-delimiter",
                        )
                    })?;
                }
                if let Some(terminator) = self.csv_terminator {
                    format.chars.terminator = Some(terminator.try_into().map_err(|err| {
                        error::user(
                            &format!("Invalid CSV terminator: {err}"),
                            "Please provide a valid CSV terminator with --csv-terminator",
                        )
                    })?);
                }
                if let Some(quote) = self.csv_quote {
                    format.chars.quote = quote.try_into().map_err(|err| {
                        error::user(
                            &format!("Invalid CSV quote: {err}"),
                            "Please provide a valid CSV quote with --csv-quote",
                        )
                    })?;
                }
                if let Some(escape) = self.csv_escape {
                    format.chars.escape = Some(escape.try_into().map_err(|err| {
                        error::user(
                            &format!("Invalid CSV escape: {err}"),
                            "Please provide a valid CSV escape with --csv-escape",
                        )
                    })?);
                }
                if let Some(comment) = self.csv_comment {
                    format.chars.comment = Some(comment.try_into().map_err(|err| {
                        error::user(
                            &format!("Invalid CSV comment: {err}"),
                            "Please provide a valid CSV comment with --csv-comment",
                        )
                    })?);
                }
                if let Some(null_regex) = self.csv_null_regex.as_ref() {
                    format.regex.null_regex = Some(Regex::new(null_regex).map_err(|err| {
                        error::user(
                            &format!("Invalid CSV null regex: {err}"),
                            "Please provide a valid CSV null regex with --csv-null-regex",
                        )
                    })?);
                }
                if let Some(true_regex) = self.csv_true_regex.as_ref() {
                    format.regex.true_regex = Some(Regex::new(true_regex).map_err(|err| {
                        error::user(
                            &format!("Invalid CSV true regex: {err}"),
                            "Please provide a valid CSV true regex with --csv-true-regex",
                        )
                    })?);
                }
                if let Some(false_regex) = self.csv_false_regex.as_ref() {
                    format.regex.false_regex = Some(Regex::new(false_regex).map_err(|err| {
                        error::user(
                            &format!("Invalid CSV false regex: {err}"),
                            "Please provide a valid CSV false regex with --csv-false-regex",
                        )
                    })?);
                }
                format.date = date_parse;
                Format::Csv(format)
            }
        };
        Ok(FormatReader::new(file, format))
    }
}

pub fn render_schema(output: SchemaOutput, global: &GlobalArgs, schema: &Schema) -> Result<String> {
    Ok(match output {
        SchemaOutput::Table => {
            let mut table = global.table();
            let fields = schema
                .fields()
                .iter()
                .map(|f| aqora_data_utils::schema::marrow::datatypes::Field::try_from(f.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| error::system("Failed to create schema table", &err.to_string()))?;
            let has_nullable = fields_into_table(&mut table, fields)
                .map_err(|err| error::system("Failed to create schema table", &err.to_string()))?;
            format!(
                "{}{}",
                table,
                if has_nullable { "\n* Nullable" } else { "" }
            )
        }
        SchemaOutput::Json => serde_json::to_string_pretty(&schema)
            .map_err(|err| error::system("Failed to create schema JSON", &err.to_string()))?,
    })
}

pub fn render_sample_debug(
    output: SchemaOutput,
    global: &GlobalArgs,
    sample_debug: SampleDebug,
    samples: &[ProcessItem<Value>],
) -> Result<String> {
    let SampleDebug { schema, error } = sample_debug;
    if schema.is_none() && error.is_none() {
        return Err(error::user("No records found", "Try with another file"));
    }
    let mut out = String::new();
    if let Some(schema) = schema {
        out.push_str(&format!(
            "Parsed the following schema:\n\n{}\n\n",
            render_schema(output, global, &schema)
                .unwrap_or("Failed to display schema".to_string())
        ));
    }
    if let Some((i, error)) = error {
        out.push_str(&format!(
            "But encountered the following error:\n\n{error}\n\n",
        ));
        out.push_str(&format!("when processing record #{}\n\n", i + 1));
        if let Some(value) = samples.get(i) {
            out.push_str(&format!(
                "{}\n\n",
                serde_json::to_string_pretty(value).unwrap_or("Error displaying record".into())
            ));
        } else {
            out.push_str("No record found\n\n");
        }
    }
    Ok(out)
}

pub struct OpenOptions {
    pub format: FormatOptions,
    pub infer: InferOptions,
    pub read: read::Options,
    pub schema: Option<String>,
    pub progress: Option<ProgressBar>,
}

type BoxRecordBatchStream = BoxStream<'static, aqora_data_utils::Result<RecordBatch>>;

enum OpenReturn {
    Ok(Schema, BoxRecordBatchStream),
    Debug(SampleDebug, Vec<ProcessItem<Value>>),
}

async fn do_open(path: impl AsRef<Path>, options: OpenOptions) -> Result<OpenReturn> {
    let path = path.as_ref();
    let meta = tokio::fs::metadata(path).await.ok();
    let schema: Option<Schema> = options
        .schema
        .as_deref()
        .map(from_json_str_or_file)
        .transpose()?;
    let mut stream = if meta.as_ref().is_some_and(|meta| meta.is_file()) {
        let meta = meta.unwrap();
        let mut file = tokio::fs::File::open(path).await.map_err(|e| {
            error::user(
                &format!("Could not open file: {}", path.display()),
                &format!("Error: {}", e),
            )
        })?;
        if let Some(progress) = &options.progress {
            progress.set_length(meta.len());
        }
        let is_parquet = is_parquet(&mut file).await.map_err(|err| {
            error::user(
                &format!("Could not read file: {err}"),
                "Check the file and try again",
            )
        })?;
        file.rewind().await?;
        if is_parquet {
            let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::load_async(
                &mut file,
                Default::default(),
            )
            .await
            .map_err(|err| {
                error::user(
                    &format!("Could not read parquet metadata: {err}"),
                    "Check the file and try again",
                )
            })?;
            let file_schema = Schema::from(Arc::clone(metadata.schema()));
            if schema.is_some_and(|schema| schema != file_schema) {
                return Err(error::user(
                    "Schema provided does not match parquet schema",
                    "Please provide a matching schema or no schema",
                ));
            }
            let progress = options.progress.clone();
            let stream =
                parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                    file.inspect(move |range| {
                        if let Some(progress) = &progress {
                            progress.set_position(range.end);
                        }
                    })
                    .merge_ranges(),
                    metadata,
                )
                .build()
                .map_err(|err| {
                    error::user(
                        &format!("Failed to build parquet stream: {err}"),
                        "Check file and try again",
                    )
                })?
                .map_err(aqora_data_utils::error::Error::from);
            return Ok(OpenReturn::Ok(file_schema, stream.boxed()));
        }
        options
            .format
            .open(path, file)
            .await?
            .into_value_stream()
            .await
            .map_err(|err| {
                error::user(
                    &format!("Could not read from input file: {err}"),
                    "Please check the file and try again",
                )
            })?
            .map_err(aqora_data_utils::error::Error::from)
            .boxed()
    } else {
        let mut glob_path = path.to_path_buf();
        if meta.is_some_and(|meta| meta.is_dir()) {
            glob_path.push("**");
        }
        let glob_str = glob_path.as_os_str().to_str().ok_or_else(|| {
            error::user(
                "Could not read glob",
                "Please make sure the glob is valid UTF-8",
            )
        })?;
        let dir_options = DirReaderOptions::new(glob_str).map_err(|err| {
            error::user(
                &format!("Failed to parse glob: {err}"),
                "Please make sure the glob is valid",
            )
        })?;
        let paths = dir_options
            .paths()
            .map(|path| {
                let glob_path = glob_path.clone();
                async move {
                    let path = path.map_err(|err| {
                        error::user(
                            &format!("Could not walk glob {}: {}", glob_path.display(), err),
                            "Check the glob and try again",
                        )
                    })?;
                    tokio::fs::metadata(&path)
                        .await
                        .map_err(|err| {
                            error::user(
                                &format!("Error reading metadata of {}: {}", path.display(), err),
                                "Check file and try again",
                            )
                        })
                        .map(|meta| (path, meta))
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;
        if paths.is_empty() {
            return Err(error::user(
                "Glob doesn't match any files",
                "Check the glob and try again",
            ));
        }
        if let Some(progress) = &options.progress {
            progress.set_length(paths.iter().map(|(_, meta)| meta.len()).sum::<u64>());
        }
        dir_options.stream_values_from_fs()
    };
    if let Some(progress) = options.progress.clone() {
        stream = stream
            .inspect_ok(move |item| progress.set_position(item.end))
            .boxed();
    }
    let schema = if let Some(schema) = schema {
        schema
    } else {
        let infer_options = options.infer.parse()?;
        let samples = stream
            .take_samples(options.infer.max_samples())
            .await
            .map_err(|err| {
                error::user(
                    &format!("Error reading from input file: {err}"),
                    "Please check the file and try again",
                )
            })?;
        let schema = if let Ok(schema) = infer::from_samples(&samples, infer_options.clone()) {
            schema
        } else {
            return Ok(OpenReturn::Debug(
                infer::debug_samples(&samples, infer_options),
                samples,
            ));
        };
        stream = futures::stream::iter(samples.into_iter().map(aqora_data_utils::Result::Ok))
            .chain(stream)
            .boxed();
        schema
    };
    let record_batches = stream
        .into_record_batch_stream(schema.clone(), options.read)
        .map_err(|err| {
            error::user(
                &format!("Error reading from input file: {err}"),
                "Please check the file and try again",
            )
        })?;
    Ok(OpenReturn::Ok(schema, record_batches.wrap().boxed()))
}

pub async fn open(
    path: impl AsRef<Path>,
    options: OpenOptions,
    global: &GlobalArgs,
    output: SchemaOutput,
) -> Result<(Schema, BoxRecordBatchStream)> {
    let progress = options.progress.clone();
    let (rendered, res) = match do_open(path, options).await? {
        OpenReturn::Ok(schema, stream) => {
            let rendered = render_schema(output, global, &schema)?;
            (rendered, Ok((schema, stream)))
        }
        OpenReturn::Debug(sample_debug, samples) => {
            let rendered = render_sample_debug(output, global, sample_debug, &samples)?;
            (
                rendered,
                Err(error::user(
                    "Could not infer the schema from the file given",
                    "Please make sure the data is conform or set overwrites with --overwrites",
                )),
            )
        }
    };
    if let Some(progress) = progress {
        progress.println(rendered);
    } else {
        println!("{rendered}");
    }
    res
}

pub async fn infer(args: Infer, global: GlobalArgs) -> Result<()> {
    let pb = global
        .spinner()
        .with_message(format!("Reading {}", args.file.display()));
    let _ = open(
        &args.file,
        OpenOptions {
            format: *args.format,
            infer: *args.infer,
            read: Default::default(),
            progress: Some(pb.clone()),
            schema: None,
        },
        &global,
        args.output,
    )
    .await?;
    pb.finish_and_clear();
    Ok(())
}
