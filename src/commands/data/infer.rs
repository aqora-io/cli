use std::path::{Path, PathBuf};

use aqora_data_utils::infer::SampleDebug;
use aqora_data_utils::{
    csv::CsvFormat,
    infer::{self, Schema},
    json::JsonItemType,
    value::Value,
    DateParseOptions, Format, FormatReader,
};
use clap::{Args, ValueEnum};
use futures::{StreamExt, TryStreamExt};
use regex::Regex;
use serde::Serialize;
use tokio::io::AsyncSeekExt;

use crate::error::{self, Result};

use super::utils::{fields_into_table, from_json_str_or_file};
use super::GlobalArgs;

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum SchemaOutput {
    Table,
    Json,
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum FileTypeArg {
    Json,
    Jsonl,
    Csv,
}

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum JsonItemTypeArg {
    Object,
    List,
}

#[derive(Args, Debug, Serialize)]
pub struct FormatOptions {
    #[arg(value_enum, long)]
    file_type: Option<FileTypeArg>,
    #[arg(value_enum, long)]
    json_item_type: Option<JsonItemTypeArg>,
    #[arg(long)]
    no_headers: bool,
    #[arg(long)]
    date_fmt: Option<String>,
    #[arg(long)]
    timestamp_fmt: Option<String>,
    #[arg(long, default_value_t = ',')]
    csv_delimiter: char,
    #[arg(long)]
    csv_terminator: Option<char>,
    #[arg(long, default_value_t = '"')]
    csv_quote: char,
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
    allow_null_fields: bool,
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
    no_allow_to_string: bool,
    #[arg(long)]
    no_guess_dates: bool,
    #[arg(long, default_value_t = 100)]
    from_type_budget: usize,
    #[arg(long)]
    enums_without_data_as_strings: bool,
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
        let mut options = infer::Options::default()
            .allow_null_fields(self.allow_null_fields)
            .coerce_numbers(!self.no_coerce_numbers)
            .allow_to_string(!self.no_allow_to_string)
            .map_as_struct(!self.no_map_as_struct)
            .sequence_as_large_list(self.large_lists)
            .strings_as_large_utf8(self.large_utf8)
            .enums_without_data_as_strings(self.enums_without_data_as_strings)
            .string_dictionary_encoding(self.string_dict_encoding)
            .guess_dates(!self.no_guess_dates)
            .from_type_budget(self.from_type_budget);
        if let Some(overwrites) = self.overwrite.as_ref() {
            let overwrites: serde_json::Map<String, serde_json::Value> =
                from_json_str_or_file(overwrites)?;
            for (name, value) in overwrites {
                options = options.overwrite(&name, value).map_err(|e| {
                    error::user(&format!("Invalid overwrite '{name}'"), &e.to_string())
                })?;
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
            match path.extension().and_then(|s| s.to_str()) {
                Some("json") => FileTypeArg::Json,
                Some("jsonl") => FileTypeArg::Jsonl,
                Some("csv") => FileTypeArg::Csv,
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
            FileTypeArg::Json | FileTypeArg::Jsonl => {
                let mut json_format = aqora_data_utils::json::infer_format(&mut file)
                    .await
                    .map_err(|e| {
                        error::user(
                            &format!("Could not infer JSON item type: {}", path.display()),
                            &format!("Error: {}", e),
                        )
                    })?;
                file.rewind().await.map_err(|e| {
                    error::user(
                        &format!("Could not rewind file: {}", path.display()),
                        &format!("Error: {}", e),
                    )
                })?;
                if self.no_headers {
                    if let JsonItemType::List {
                        ref mut has_headers,
                    } = json_format.item_type
                    {
                        *has_headers = false;
                    }
                }
                json_format.date_parse = date_parse;
                Format::Json(json_format)
            }
            FileTypeArg::Csv => Format::Csv(CsvFormat {
                has_headers: !self.no_headers,
                delimiter: self.csv_delimiter.try_into().map_err(|err| {
                    error::user(
                        &format!("Invalid CSV delimiter: {err}"),
                        "Please provide a valid CSV delimiter with --csv-delimiter",
                    )
                })?,
                terminator: self
                    .csv_terminator
                    .map(|t| {
                        t.try_into().map_err(|err| {
                            error::user(
                                &format!("Invalid CSV terminator: {err}"),
                                "Please provide a valid CSV terminator with --csv-terminator",
                            )
                        })
                    })
                    .transpose()?,
                quote: self.csv_quote.try_into().map_err(|err| {
                    error::user(
                        &format!("Invalid CSV quote: {err}"),
                        "Please provide a valid CSV quote with --csv-quote",
                    )
                })?,
                escape: self
                    .csv_escape
                    .map(|e| {
                        e.try_into().map_err(|err| {
                            error::user(
                                &format!("Invalid CSV escape: {err}"),
                                "Please provide a valid CSV escape with --csv-escape",
                            )
                        })
                    })
                    .transpose()?,
                comment: self
                    .csv_comment
                    .map(|c| {
                        c.try_into().map_err(|err| {
                            error::user(
                                &format!("Invalid CSV comment: {err}"),
                                "Please provide a valid CSV comment with --csv-comment",
                            )
                        })
                    })
                    .transpose()?,
                null_regex: self
                    .csv_null_regex
                    .as_ref()
                    .map(|re| {
                        Regex::new(re).map_err(|err| {
                            error::user(
                                &format!("Invalid CSV null regex: {err}"),
                                "Please provide a valid CSV null regex with --csv-null-regex",
                            )
                        })
                    })
                    .transpose()?,
                true_regex: self
                    .csv_true_regex
                    .as_ref()
                    .map(|re| {
                        Regex::new(re).map_err(|err| {
                            error::user(
                                &format!("Invalid CSV true regex: {err}"),
                                "Please provide a valid CSV true regex with --csv-true-regex",
                            )
                        })
                    })
                    .transpose()?,
                false_regex: self
                    .csv_false_regex
                    .as_ref()
                    .map(|re| {
                        Regex::new(re).map_err(|err| {
                            error::user(
                                &format!("Invalid CSV false regex: {err}"),
                                "Please provide a valid CSV false regex with --csv-false-regex",
                            )
                        })
                    })
                    .transpose()?,
                date_parse,
            }),
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
                .map(|f| aqora_data_utils::infer::marrow::datatypes::Field::try_from(f.as_ref()))
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
    samples: &[Value],
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
