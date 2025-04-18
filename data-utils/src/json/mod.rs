pub mod reader;
pub use reader::{JsonProcessor, JsonReadStream};

use futures::prelude::*;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::value::{DateParseOptions, Value, ValueExt};
use crate::Format;

#[derive(Default, Debug, Clone, Copy)]
pub enum JsonFileType {
    #[default]
    Json,
    Jsonl,
}

#[derive(Default, Debug, Clone, Copy)]
pub enum JsonItemType {
    #[default]
    Object,
    List {
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

#[derive(Default, Debug, Clone)]
pub struct JsonFormat {
    pub file_type: JsonFileType,
    pub item_type: JsonItemType,
    pub date_parse: DateParseOptions,
}

impl From<JsonFormat> for Format {
    fn from(value: JsonFormat) -> Self {
        Self::Json(value)
    }
}

pub async fn read<'a, R>(
    mut reader: R,
    options: JsonFormat,
) -> io::Result<impl Stream<Item = io::Result<Value>> + 'a>
where
    R: AsyncRead + AsyncSeek + Unpin + 'a,
{
    let headers = if options.item_type.has_headers() {
        let headers = JsonReadStream::<_, Vec<String>>::new(
            &mut reader,
            JsonProcessor::new(options.file_type),
        )
        .map_ok(|(_, v)| v)
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
            res.and_then(|(_key, value)| io::Result::Ok(value.with_headers(headers.as_ref())?))
        })
        .boxed_local();
    if has_headers {
        stream = stream.skip(1).boxed_local();
    }
    if !options.date_parse.is_empty() {
        stream = stream
            .map_ok(move |v| {
                v.map_values(|v| match v {
                    Value::String(s) => Value::String(options.date_parse.normalize(s)),
                    _ => v,
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
    let mut types_to_guess = [JsonFileType::Json, JsonFileType::Jsonl]
        .into_iter()
        .peekable();
    while let Some(file_type) = types_to_guess.next() {
        let item =
            JsonReadStream::<_, serde_json::Value>::new(&mut reader, JsonProcessor::new(file_type))
                .try_next()
                .await;
        let item_type = match item {
            Ok(Some((_, serde_json::Value::Object(_)))) => JsonItemType::Object,
            Ok(Some((_, serde_json::Value::Array(arr)))) => JsonItemType::List {
                has_headers: arr.iter().all(|v| v.is_string()),
            },
            _ => {
                if types_to_guess.peek().is_some() {
                    reader.rewind().await?;
                }
                continue;
            }
        };
        return Ok(JsonFormat {
            file_type,
            item_type,
            date_parse: DateParseOptions::default(),
        });
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "No valid format found",
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{format::AsyncFileReader, infer, FormatReader};

    pub fn load_json(path: &str, format: JsonFormat) -> FormatReader<impl AsyncFileReader> {
        FormatReader::new(
            std::io::Cursor::new(std::fs::read(format!("tests/data/json/{path}")).unwrap()),
            format.into(),
        )
    }

    fn infer_options() -> infer::Options {
        infer::Options::new().coerce_numbers(true).guess_dates(true)
    }

    #[tokio::test]
    async fn basic_json() {
        println!(
            "basic_json: {:#?}",
            load_json("basic.json", JsonFormat::default())
                .infer_schema(infer_options(), None)
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "fs")]
    #[tokio::test]
    async fn convert_basic_json() {
        FormatReader::new(
            tokio::fs::File::open("tests/data/json/basic.json")
                .await
                .unwrap(),
            JsonFormat::default().into(),
        )
        .infer_and_stream_record_batches(infer_options(), None, Default::default())
        .await
        .unwrap()
        .write_to(
            tokio::fs::File::create("/tmp/convert_basic_json.test.parquet")
                .await
                .unwrap(),
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
            .infer_schema(infer_options(), None)
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
            .infer_schema(infer_options(), None)
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
            .infer_schema(infer_options(), None)
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
            .infer_schema(infer_options(), None)
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
            .infer_schema(infer_options(), None)
            .await
            .unwrap()
        );
    }
}
