use std::collections::HashMap;
use std::path::{Path, PathBuf};

use aqora_data_utils::infer::SampleDebug;
use aqora_data_utils::{
    csv::{infer_format as infer_csv_format, CsvFormat},
    infer::{self},
    json::{infer_format as infer_json_format, JsonFileOptions, JsonFormat, JsonItemOptions},
    schema::Schema,
    value::Value,
    DateParseOptions, Format, FormatReader, ProcessItem,
};
use clap::{Args, ValueEnum};
use futures::{StreamExt, TryStreamExt};
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
    large_lists: bool,
    #[arg(long)]
    large_utf8: bool,
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
            sequence_as_large_list: self.large_lists,
            string_as_large_utf8: self.large_utf8,
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
    pub async fn open(&self, path: impl AsRef<Path>) -> Result<FormatReader<tokio::fs::File>> {
        let path = path.as_ref();
        let mut file = tokio::fs::File::open(path).await.map_err(|e| {
            error::user(
                &format!("Could not open file: {}", path.display()),
                &format!("Error: {}", e),
            )
        })?;
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

pub async fn infer(args: Infer, global: GlobalArgs) -> Result<()> {
    let pb = global
        .spinner()
        .with_message(format!("Reading {}", args.file.display()));
    let infer_options = args.infer.parse()?;
    let mut reader = args.format.open(&args.file).await?;
    let mut stream = reader
        .stream_values()
        .await
        .map_err(|e| error::user("Could not read values from file", &format!("Error: {}", e)))?
        .boxed_local();
    if let Some(max_samples) = args.infer.max_samples() {
        stream = stream.take(max_samples).boxed_local();
    }
    let values = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| error::user("Could not read values from file", &format!("Error: {}", e)))?;
    if let Ok(schema) = infer::from_samples(&values, infer_options.clone()) {
        pb.finish();
        pb.finish_and_clear();
        println!("{}", render_schema(args.output, &global, &schema)?)
    } else {
        pb.println(render_sample_debug(
            args.output,
            &global,
            infer::debug_samples(&values, infer_options),
            &values,
        )?);
        return Err(error::user(
            "Could not infer the schema from the file given",
            "Please make sure the data is conform or set overwrites with --overwrites",
        ));
    }
    Ok(())
}
