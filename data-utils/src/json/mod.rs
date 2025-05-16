use std::collections::HashSet;

use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::process::ProcessItem;
use crate::value::{DateParseOptions, Value, ValueExt};
use crate::Format;

pub mod reader;
pub use reader::{JsonProcessor, JsonReadStream};

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(export, export_to = "bindings.ts")
)]
#[serde(rename_all = "snake_case")]
pub enum JsonFileType {
    #[default]
    Json,
    Jsonl,
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(export, export_to = "bindings.ts")
)]
#[serde(tag = "item_type", rename_all = "snake_case")]
pub enum JsonItemType {
    #[default]
    Object,
    List {
        #[serde(default)]
        #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
        has_headers: bool,
    },
}

impl JsonItemType {
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
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(export, export_to = "bindings.ts")
)]
pub struct JsonFormat {
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<JsonFileType>"))]
    pub file_type: JsonFileType,
    #[serde(flatten)]
    pub item_type: JsonItemType,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<DateParseOptions>"))]
    pub date_parse: DateParseOptions,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub key_col: Option<String>,
}

impl From<JsonFormat> for Format {
    fn from(value: JsonFormat) -> Self {
        Self::Json(value)
    }
}

pub async fn read<'a, R>(
    mut reader: R,
    options: JsonFormat,
) -> io::Result<impl Stream<Item = io::Result<ProcessItem<Value>>> + 'a>
where
    R: AsyncRead + AsyncSeek + Unpin + 'a,
{
    let headers = if options.item_type.has_headers() {
        let headers = JsonReadStream::<_, Vec<String>>::new(
            &mut reader,
            JsonProcessor::new(options.file_type),
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
    let mut stream = JsonReadStream::<_, Value>::new(reader, JsonProcessor::new(options.file_type))
        .map(move |res| {
            res.and_then(|item| {
                item.map(|(key, value)| -> io::Result<Value> {
                    let value = value.with_headers(headers.as_ref())?;
                    if let (Some(key_col), Some(key)) = (options.key_col.as_ref(), key) {
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
        })
        .boxed_local();
    if has_headers {
        stream = stream.skip(1).boxed_local();
    }
    if !options.date_parse.is_empty() {
        stream = stream
            .map_ok(move |item| {
                item.map(|v| {
                    v.map_values(|v| match v {
                        Value::String(s) => Value::String(options.date_parse.normalize(s)),
                        _ => v,
                    })
                })
            })
            .boxed_local()
    }
    Ok(stream.boxed_local())
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
                                Some((JsonItemType::Object, None))
                            } else {
                                let headers =
                                    map.keys().map(|k| k.to_lowercase()).collect::<HashSet<_>>();
                                let key_col = key_cols.into_iter().find(|k| !headers.contains(*k));
                                Some((JsonItemType::Object, key_col))
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
                                    Some((JsonItemType::List { has_headers: true }, None))
                                } else {
                                    let key_col =
                                        key_cols.into_iter().find(|k| !headers.contains(*k));
                                    Some((JsonItemType::List { has_headers: true }, key_col))
                                }
                            } else {
                                Some((JsonItemType::List { has_headers: false }, Some("key")))
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
        if let Some((item_type, key_col)) = inferred {
            return Ok(JsonFormat {
                file_type,
                item_type,
                date_parse: DateParseOptions::default(),
                key_col: key_col.map(|k| k.to_string()),
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
    use crate::{format::AsyncFileReader, FormatReader};

    pub fn load_json(path: &str, format: JsonFormat) -> FormatReader<impl AsyncFileReader> {
        FormatReader::new(
            std::io::Cursor::new(std::fs::read(format!("tests/data/json/{path}")).unwrap()),
            format.into(),
        )
    }

    #[tokio::test]
    async fn basic_json() {
        println!(
            "basic_json: {:#?}",
            load_json("basic.json", JsonFormat::default())
                .infer_schema(Default::default(), None)
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "fs")]
    #[tokio::test]
    async fn convert_basic_json() {
        use crate::write::SinglePart;
        FormatReader::new(
            tokio::fs::File::open("tests/data/json/basic.json")
                .await
                .unwrap(),
            JsonFormat::default().into(),
        )
        .infer_and_stream_record_batches(Default::default(), None, Default::default())
        .await
        .unwrap()
        .write_to_parquet(
            SinglePart::new(
                tokio::fs::File::create("/tmp/convert_basic_json.test.parquet")
                    .await
                    .unwrap(),
            ),
            Default::default(),
        )
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
                    file_type: JsonFileType::Jsonl,
                    ..Default::default()
                }
            )
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
                    item_type: JsonItemType::List { has_headers: true },
                    ..Default::default()
                }
            )
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
                    item_type: JsonItemType::List { has_headers: true },
                    file_type: JsonFileType::Jsonl,
                    ..Default::default()
                }
            )
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
                    item_type: JsonItemType::List { has_headers: false },
                    ..Default::default()
                }
            )
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
                    item_type: JsonItemType::List { has_headers: false },
                    file_type: JsonFileType::Jsonl,
                    ..Default::default()
                }
            )
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }
}
