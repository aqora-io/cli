pub mod reader;

use std::iter::repeat_n;

pub use reader::{JsonReadStream, JsonlReadStream};

use arrow::datatypes::Schema;
use futures::{prelude::*, stream::LocalBoxStream};
use serde::de::DeserializeOwned;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::schema::{CollectedType, InferredType, Primitive};
use crate::InferResult;

#[derive(Default, Debug, Clone, Copy)]
pub enum FileType {
    #[default]
    Json,
    Jsonl,
}

#[derive(Default, Debug, Clone, Copy)]
pub enum ItemType {
    #[default]
    Object,
    List {
        has_headers: bool,
    },
}

#[derive(Default, Debug, Clone, Copy)]
pub struct Format {
    pub file_type: FileType,
    pub item_type: ItemType,
}

fn json_stream<'a, T, R>(file_type: FileType, reader: R) -> LocalBoxStream<'a, io::Result<T>>
where
    T: DeserializeOwned + 'a,
    R: AsyncRead + 'a,
{
    match file_type {
        FileType::Json => JsonReadStream::new_default(reader).boxed_local(),
        FileType::Jsonl => JsonlReadStream::new_default(reader).boxed_local(),
    }
}

pub async fn infer_schema<R>(
    mut reader: R,
    options: &Format,
    max_records: Option<usize>,
) -> InferResult<Schema>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let headers = if matches!(options.item_type, ItemType::List { has_headers: true }) {
        let mut stream = json_stream::<Vec<String>, _>(options.file_type, &mut reader);
        let headers = stream.try_next().await?;
        drop(stream);
        reader.rewind().await?;
        headers
    } else {
        None
    };
    let has_headers = headers.is_some();
    let mut stream = json_stream::<InferredType<Primitive>, _>(options.file_type, reader)
        .map(|ty| match ty? {
            ty @ InferredType::Struct(_) => Ok(ty),
            InferredType::List(types) => {
                let headers = headers
                    .clone()
                    .unwrap_or_else(|| (0..types.len()).map(|s| s.to_string()).collect());
                if headers.len() < types.len() {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Headers do not match data length",
                    ))
                } else {
                    let extra_values_n = headers.len() - types.len();
                    Ok(InferredType::Struct(
                        headers
                            .into_iter()
                            .zip(
                                types
                                    .into_iter()
                                    .chain(repeat_n(InferredType::Null, extra_values_n)),
                            )
                            .collect(),
                    ))
                }
            }
            ty => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected struct or list. Received {ty:?}"),
            )),
        })
        .boxed_local();
    if let Some(max_records) = max_records {
        stream = stream.take(max_records).boxed_local();
    };
    if has_headers {
        stream = stream.skip(1).boxed_local();
    }
    Ok(stream
        .try_collect::<CollectedType<_>>()
        .await?
        .into_io_result()?
        .into_schema()
        .map_err(io::Error::from)?)
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::fs::File;

    #[tokio::test]
    async fn basic_json() {
        println!(
            "basic_json: {:#?}",
            infer_schema(
                File::open("tests/data/json/basic.json").await.unwrap(),
                &Default::default(),
                None,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_jsonl() {
        println!(
            "basic_jsonl: {:#?}",
            infer_schema(
                File::open("tests/data/json/basic.jsonl").await.unwrap(),
                &Format {
                    file_type: FileType::Jsonl,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_json() {
        println!(
            "basic_lists_json: {:#?}",
            infer_schema(
                File::open("tests/data/json/basic_lists.json")
                    .await
                    .unwrap(),
                &Format {
                    item_type: ItemType::List { has_headers: true },
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_jsonl() {
        println!(
            "basic_lists_jsonl: {:#?}",
            infer_schema(
                File::open("tests/data/json/basic_lists.jsonl")
                    .await
                    .unwrap(),
                &Format {
                    item_type: ItemType::List { has_headers: true },
                    file_type: FileType::Jsonl,
                },
                None,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_no_headers_json() {
        println!(
            "basic_lists_no_headers_json: {:#?}",
            infer_schema(
                File::open("tests/data/json/basic_lists_no_headers.json")
                    .await
                    .unwrap(),
                &Format {
                    item_type: ItemType::List { has_headers: false },
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_lists_no_headers_jsonl() {
        println!(
            "{:#?}",
            infer_schema(
                File::open("tests/data/json/basic_lists_no_headers.jsonl")
                    .await
                    .unwrap(),
                &Format {
                    item_type: ItemType::List { has_headers: false },
                    file_type: FileType::Jsonl,
                },
                None,
            )
            .await
            .unwrap()
        );
    }
}
