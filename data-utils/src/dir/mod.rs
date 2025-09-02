mod glob;

use glob::utils::path_buf_to_string;
pub use glob::{GlobError, GlobPath};

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use futures::{prelude::*, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};

use crate::async_util::parquet_async::*;
use crate::error::{Error, Result};
use crate::process::{ProcessItem, ProcessItemStream};
use crate::value::{DateParseOptions, Map, Value};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
pub struct DirReaderOptions {
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub file_key: Option<String>,
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub path_key: Option<String>,
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<DateParseOptions>"))]
    pub date: DateParseOptions,
    #[cfg_attr(feature = "wasm", ts(as = "String"))]
    pub glob: GlobPath,
}

const DEFAULT_DECORATORS: &[(&str, &str)] =
    &[("", ""), ("_", ""), ("", "_"), ("_", "_"), ("__", "__")];
const DEFAULT_FILE_KEYS: &[&str] = &["data", "content", "contents", "blob"];
const DEFAULT_PATH_KEYS: &[&str] = &["path", "file", "filepath", "location"];

fn find_default_key(names: &HashSet<&str>, options: &[&str]) -> Option<String> {
    for (prefix, suffix) in DEFAULT_DECORATORS {
        for key in options {
            let composed = format!("{prefix}{key}{suffix}");
            if !names.contains(&composed.as_str()) {
                return Some(composed);
            }
        }
    }
    None
}

fn to_value(value: String, date: &DateParseOptions) -> Value {
    if let Ok(b) = value.parse::<bool>() {
        b.into()
    } else if let Ok(i) = value.parse::<i64>() {
        i.into()
    } else if let Ok(f) = value.parse::<f64>() {
        f.into()
    } else {
        date.normalize(value).into()
    }
}

impl DirReaderOptions {
    pub fn new(glob: impl AsRef<str>) -> Result<Self, GlobError> {
        let glob = glob.as_ref().parse::<GlobPath>()?;
        let names = glob.names().collect::<HashSet<_>>();
        Ok(Self {
            file_key: find_default_key(&names, DEFAULT_FILE_KEYS),
            path_key: find_default_key(&names, DEFAULT_PATH_KEYS),
            date: DateParseOptions::default(),
            glob,
        })
    }

    fn do_stream_values<I, F, Fut>(
        &self,
        matches: I,
        mut read: F,
    ) -> ProcessItemStream<'static, Value, Error>
    where
        I: IntoIterator<Item = Result<(PathBuf, Vec<(String, Option<String>)>), GlobError>>,
        F: FnMut(PathBuf) -> Fut,
        Fut: TryFuture<Ok = Vec<u8>> + MaybeSend + 'static,
        Error: From<Fut::Error>,
    {
        let mut bytes = 0;
        boxed_stream(
            matches
                .into_iter()
                .map(|item| {
                    let file_key = self.file_key.clone();
                    let path_key = self.path_key.clone();
                    let date = self.date.clone();
                    let item = item.map(|(path, mat)| (path.clone(), mat, read(path)));
                    async move {
                        let (path, mat, data) = item?;
                        let mut map = mat
                            .into_iter()
                            .map(|(k, v)| (k, v.map(|v| to_value(v, &date))))
                            .collect::<Map>();
                        let size = if let Some(file_key) = file_key {
                            let data = data.into_future().await?;
                            let size = data.len();
                            let data = match String::from_utf8(data) {
                                Ok(string) => Value::String(string),
                                Err(err) => Value::Bytes(err.into_bytes()),
                            };
                            map.insert(file_key, data);
                            size
                        } else {
                            0
                        };
                        if let Some(path_key) = path_key {
                            map.insert(path_key, path_buf_to_string(path)?);
                        }
                        Result::<_, Error>::Ok((map.into(), size))
                    }
                })
                .collect::<FuturesUnordered<_>>()
                .map_ok(move |(item, size)| {
                    let start = bytes;
                    bytes += size as u64;
                    ProcessItem {
                        start,
                        end: bytes,
                        item,
                    }
                }),
        )
    }

    #[cfg(feature = "fs")]
    pub fn paths(&self) -> impl Iterator<Item = Result<PathBuf, GlobError>> {
        self.glob.clone().walk().map(|item| Ok(item?.0))
    }

    #[cfg(feature = "fs")]
    pub fn stream_values_from_fs(&self) -> ProcessItemStream<'static, Value, Error> {
        use tokio::{
            fs::File,
            io::{self, AsyncReadExt},
        };
        self.do_stream_values(self.glob.clone().walk(), |path| async move {
            let mut bytes = Vec::new();
            File::open(&path).await?.read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        })
    }

    pub fn stream_values<I, F, Fut>(
        &self,
        paths: I,
        read: F,
    ) -> ProcessItemStream<'static, Value, Error>
    where
        I: IntoIterator,
        I::Item: AsRef<Path>,
        F: FnMut(PathBuf) -> Fut,
        Fut: TryFuture<Ok = Vec<u8>> + MaybeSend + 'static,
        Error: From<Fut::Error>,
    {
        let glob = self.glob.clone();
        self.do_stream_values(
            paths.into_iter().filter_map(move |file| {
                Some(glob.matches(file.as_ref()).transpose()?.map(|mat| {
                    let mat_owned = mat.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
                    (file.as_ref().to_owned(), mat_owned)
                }))
            }),
            read,
        )
    }
}

impl Default for DirReaderOptions {
    fn default() -> Self {
        Self::new("**").unwrap()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "fs")]
    #[tokio::test]
    async fn dir_reader_stream_values_simple() {
        use super::*;
        let glob = DirReaderOptions::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/files/dir/simple/{split}/{animal}/{name}.json",
        ))
        .unwrap();
        for item in glob
            .stream_values_from_fs()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            println!("{:?}", item);
        }
    }
}
