use std::collections::HashSet;

use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::async_util::parquet_async::{boxed_stream, MaybeSend};
use crate::format::Format;
use crate::process::{ProcessItem, ProcessItemStream};
use crate::value::{DateParseOptions, Value, ValueExt};

pub mod reader;
pub use reader::{JsonProcessor, JsonReadStream};

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum JsonFileType {
    #[default]
    Json,
    Jsonl,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
#[serde(tag = "file_type", rename_all = "snake_case")]
pub enum JsonFileOptions {
    Json {
        #[serde(default)]
        #[cfg_attr(feature = "wasm", ts(optional))]
        key_col: Option<String>,
    },
    Jsonl,
}

impl JsonFileOptions {
    pub fn ty(&self) -> JsonFileType {
        match self {
            Self::Json { .. } => JsonFileType::Json,
            Self::Jsonl => JsonFileType::Jsonl,
        }
    }
    pub fn key_col(&self) -> Option<&str> {
        match self {
            Self::Json { key_col } => key_col.as_deref(),
            Self::Jsonl => None,
        }
    }
}

impl Default for JsonFileOptions {
    fn default() -> Self {
        Self::Json { key_col: None }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
#[serde(tag = "item_type", rename_all = "snake_case")]
pub enum JsonItemOptions {
    #[default]
    Object,
    List {
        #[serde(default)]
        #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
        has_headers: bool,
    },
}

impl JsonItemOptions {
    pub fn has_headers(&self) -> bool {
        match self {
            Self::List { has_headers } => *has_headers,
            Self::Object => false,
        }
    }

    pub fn set_has_headers(&mut self, value: bool) {
        match self {
            Self::List { has_headers } => *has_headers = value,
            Self::Object => {}
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
pub struct JsonFormat {
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<JsonFileOptions>"))]
    pub file: JsonFileOptions,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<JsonItemOptions>"))]
    pub item: JsonItemOptions,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<DateParseOptions>"))]
    pub date: DateParseOptions,
}

impl From<JsonFormat> for Format {
    fn from(value: JsonFormat) -> Self {
        Self::Json(value)
    }
}

pub async fn read<'a, R>(mut reader: R, options: JsonFormat) -> io::Result<ProcessItemStream<'a>>
where
    R: AsyncRead + AsyncSeek + MaybeSend + Unpin + 'a,
{
    let headers = if options.item.has_headers() {
        let headers = JsonReadStream::<_, Vec<String>>::new(
            &mut reader,
            JsonProcessor::new(options.file.ty()),
        )
        .map_ok(
            |ProcessItem {
                 item: (_, value), ..
             }| value,
        )
        .try_next()
        .await?;
        reader.rewind().await?;
        headers
    } else {
        None
    };
    let has_headers = headers.is_some();
    let mut stream = boxed_stream(
        JsonReadStream::<_, Value>::new(reader, JsonProcessor::new(options.file.ty())).map(
            move |res| {
                res.and_then(|item| {
                    item.map(|(key, value)| -> io::Result<Value> {
                        let value = value.with_headers(headers.as_ref())?;
                        if let (Some(key_col), Some(key)) = (options.file.key_col(), key) {
                            let key_value = match ron::from_str(&key) {
                                Ok(ron::Value::Number(num)) => ron::Value::Number(num),
                                _ => ron::Value::String(key),
                            };
                            Ok(value.with_entry(key_col.to_string(), key_value)?)
                        } else {
                            Ok(value)
                        }
                    })
                    .transpose()
                })
            },
        ),
    );
    if has_headers {
        stream = boxed_stream(stream.skip(1));
    }
    if !options.date.is_empty() {
        stream = boxed_stream(stream.map_ok(move |item| {
            item.map(|v| {
                v.map_values(|v| match v {
                    Value::String(s) => Value::String(options.date.normalize(s)),
                    _ => v,
                })
            })
        }));
    }
    Ok(stream)
}

pub async fn infer_format<R>(mut reader: R) -> io::Result<JsonFormat>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let key_cols = ["key", "_key", "__key", "_key_", "__key__"];
    for file_type in [JsonFileType::Json, JsonFileType::Jsonl] {
        let mut stream =
            JsonReadStream::<_, serde_json::Value>::new(&mut reader, JsonProcessor::new(file_type));
        let inferred = match stream.try_next().await {
            Ok(Some(ProcessItem {
                item: (key, value), ..
            })) => {
                if stream.try_next().await.is_ok() {
                    match value {
                        serde_json::Value::Object(map) => {
                            if key.is_none() {
                                Some((JsonItemOptions::Object, None))
                            } else {
                                let headers =
                                    map.keys().map(|k| k.to_lowercase()).collect::<HashSet<_>>();
                                let key_col = key_cols.into_iter().find(|k| !headers.contains(*k));
                                Some((JsonItemOptions::Object, key_col))
                            }
                        }
                        serde_json::Value::Array(arr) => {
                            let headers = arr
                                .into_iter()
                                .map(|v| match v {
                                    serde_json::Value::String(s) => Some(s),
                                    _ => None,
                                })
                                .collect::<Option<HashSet<String>>>();
                            if let Some(headers) = headers {
                                if key.is_none() {
                                    Some((JsonItemOptions::List { has_headers: true }, None))
                                } else {
                                    let key_col =
                                        key_cols.into_iter().find(|k| !headers.contains(*k));
                                    Some((JsonItemOptions::List { has_headers: true }, key_col))
                                }
                            } else {
                                Some((JsonItemOptions::List { has_headers: false }, Some("key")))
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some((item, key_col)) = inferred {
            let file = match file_type {
                JsonFileType::Json => JsonFileOptions::Json {
                    key_col: key_col.map(|s| s.to_string()),
                },
                JsonFileType::Jsonl => JsonFileOptions::Jsonl,
            };
            return Ok(JsonFormat {
                file,
                item,
                date: DateParseOptions::default(),
            });
        } else {
            reader.rewind().await?;
        };
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "No valid format found",
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{format::AsyncFileReader, read::ValueStream, FormatReader};

    pub fn load_json(path: &str, format: JsonFormat) -> FormatReader<impl AsyncFileReader> {
        FormatReader::new(
            std::io::Cursor::new(std::fs::read(format!("tests/data/files/json/{path}")).unwrap()),
            format.into(),
        )
    }

    #[tokio::test]
    async fn basic_json() {
        println!(
            "basic_json: {:#?}",
            load_json("basic.json", JsonFormat::default())
                .stream_values()
                .await
                .unwrap()
                .infer_schema(Default::default(), None)
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "fs")]
    #[tokio::test]
    async fn convert_basic_json() {
        use crate::write::{RecordBatchStreamParquetExt, SinglePart};
        FormatReader::new(
            tokio::fs::File::open("tests/data/files/json/basic.json")
                .await
                .unwrap(),
            JsonFormat::default().into(),
        )
        .stream_values()
        .await
        .unwrap()
        .into_inferred_record_batch_stream(Default::default(), None, Default::default())
        .await
        .unwrap()
        .write_to_parquet(
            SinglePart::new(
                tokio::fs::File::create("/tmp/convert_basic_json.test.parquet")
                    .await
                    .unwrap(),
            ),
            Default::default(),
            Default::default(),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn basic_jsonl() {
        println!(
            "basic_jsonl: {:#?}",
            load_json(
                "basic.jsonl",
                JsonFormat {
                    file: JsonFileOptions::Jsonl,
                    ..Default::default()
                }
            )
            .stream_values()
            .await
            .unwrap()
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_json() {
        println!(
            "basic_lists_json: {:#?}",
            load_json(
                "basic_lists.json",
                JsonFormat {
                    item: JsonItemOptions::List { has_headers: true },
                    ..Default::default()
                }
            )
            .stream_values()
            .await
            .unwrap()
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_jsonl() {
        println!(
            "basic_lists_jsonl: {:#?}",
            load_json(
                "basic_lists.jsonl",
                JsonFormat {
                    item: JsonItemOptions::List { has_headers: true },
                    file: JsonFileOptions::Jsonl,
                    ..Default::default()
                }
            )
            .stream_values()
            .await
            .unwrap()
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_no_headers_json() {
        println!(
            "basic_lists_no_headers_json: {:#?}",
            load_json(
                "basic_lists_no_headers.json",
                JsonFormat {
                    item: JsonItemOptions::List { has_headers: false },
                    ..Default::default()
                }
            )
            .stream_values()
            .await
            .unwrap()
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_no_headers_jsonl() {
        println!(
            "{:#?}",
            load_json(
                "basic_lists_no_headers.jsonl",
                JsonFormat {
                    item: JsonItemOptions::List { has_headers: false },
                    file: JsonFileOptions::Jsonl,
                    ..Default::default()
                }
            )
            .stream_values()
            .await
            .unwrap()
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }
}
