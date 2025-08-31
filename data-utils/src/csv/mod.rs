pub mod reader;
pub use reader::{CsvProcessor, CsvReadStream};

use csv_core::{Reader, ReaderBuilder, Terminator};
use futures::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::async_util::parquet_async::{boxed_stream, MaybeSend};
use crate::format::Format;
use crate::process::ProcessItemStream;
use crate::serde::{ascii_char, ascii_char_opt, regex_opt};
use crate::value::{DateParseOptions, Value, ValueExt};

const fn default_delimiter() -> u8 {
    b','
}

const fn default_quote() -> u8 {
    b'"'
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
pub struct CsvFormatChars {
    #[serde(default = "default_delimiter", with = "ascii_char")]
    #[cfg_attr(feature = "wasm", ts(as = "char"))]
    pub delimiter: u8,
    /// None is CLRF
    #[serde(default, with = "ascii_char_opt")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<char>"))]
    pub terminator: Option<u8>,
    #[serde(default = "default_quote", with = "ascii_char")]
    #[cfg_attr(feature = "wasm", ts(as = "char"))]
    pub quote: u8,
    #[serde(default, with = "ascii_char_opt")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<char>"))]
    pub escape: Option<u8>,
    #[serde(default, with = "ascii_char_opt")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<char>"))]
    pub comment: Option<u8>,
}

impl Default for CsvFormatChars {
    fn default() -> Self {
        Self {
            terminator: None,
            delimiter: default_delimiter(),
            quote: default_quote(),
            escape: None,
            comment: None,
        }
    }
}

impl From<CsvFormatChars> for Reader {
    fn from(value: CsvFormatChars) -> Self {
        let mut reader = ReaderBuilder::default();
        reader
            .delimiter(value.delimiter)
            .quote(value.quote)
            .escape(value.escape)
            .comment(value.comment)
            .terminator(
                value
                    .terminator
                    .map(Terminator::Any)
                    .unwrap_or(Terminator::CRLF),
            );
        reader.build()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CsvFormatRegex {
    #[serde(rename = "null", default, with = "regex_opt")]
    pub null_regex: Option<Regex>,
    #[serde(rename = "true", default, with = "regex_opt")]
    pub true_regex: Option<Regex>,
    #[serde(rename = "false", default, with = "regex_opt")]
    pub false_regex: Option<Regex>,
}

impl CsvFormatRegex {
    #[inline]
    pub(crate) fn check_null(&self, s: &str) -> bool {
        match self.null_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => s.is_empty(),
        }
    }
    #[inline]
    pub(crate) fn check_true(&self, s: &str) -> bool {
        match self.true_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => matches!(s, "true" | "True" | "TRUE"),
        }
    }
    #[inline]
    pub(crate) fn check_false(&self, s: &str) -> bool {
        match self.false_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => matches!(s, "false" | "False" | "FALSE"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CsvFormat {
    pub has_headers: bool,
    #[serde(default)]
    pub chars: CsvFormatChars,
    #[serde(default)]
    pub regex: CsvFormatRegex,
    #[serde(default)]
    pub date: DateParseOptions,
}

impl From<CsvFormat> for Format {
    fn from(value: CsvFormat) -> Self {
        Self::Csv(value)
    }
}

pub async fn read<'a, R>(mut reader: R, options: CsvFormat) -> io::Result<ProcessItemStream<'a>>
where
    R: AsyncRead + AsyncSeek + MaybeSend + Unpin + 'a,
{
    let headers = if options.has_headers {
        let headers =
            CsvReadStream::<_, Vec<String>>::new(&mut reader, CsvProcessor::new(options.chars))
                .map_ok(|item| item.item)
                .try_next()
                .await?;
        reader.rewind().await?;
        headers
    } else {
        None
    };
    let has_headers = headers.is_some();
    let mut stream = boxed_stream(
        CsvReadStream::<_, Vec<Value>>::new(reader, CsvProcessor::new(options.chars))
            .map(move |item| {
                Ok(item?
                    .map(|seq| Value::Seq(seq).with_headers(headers.clone()))
                    .transpose()?)
            })
            .map_ok(move |item| {
                item.map(|v| {
                    v.map_values(|v| match v {
                        Value::String(s) => {
                            if options.regex.check_null(&s) {
                                Value::Unit
                            } else if options.regex.check_true(&s) {
                                Value::Bool(true)
                            } else if options.regex.check_false(&s) {
                                Value::Bool(false)
                            } else {
                                Value::String(options.date.normalize(s))
                            }
                        }
                        _ => v,
                    })
                })
            }),
    );
    if has_headers {
        stream = boxed_stream(stream.skip(1));
    }
    Ok(stream)
}

pub async fn infer_format<R>(mut reader: R, max_records: Option<usize>) -> io::Result<CsvFormat>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let delimiters_to_guess = [b',', b'\t', b';', b'|'];
    let quotes_to_guess = [b'"', b'\''];
    let escapes_to_guess = [None, Some(b'\\')];
    let format_guesses = delimiters_to_guess
        .into_iter()
        .flat_map(|delimiter| {
            quotes_to_guess
                .into_iter()
                .map(move |quote| (delimiter, quote))
        })
        .flat_map(|(delimiter, quote)| {
            escapes_to_guess
                .into_iter()
                .map(move |escape| (delimiter, quote, escape))
        })
        .map(|(delimiter, quote, escape)| CsvFormat {
            chars: CsvFormatChars {
                delimiter,
                quote,
                escape,
                ..Default::default()
            },
            ..Default::default()
        });
    let mut best_format_guess = None;
    for format in format_guesses {
        let mut stream =
            CsvReadStream::<_, Vec<Value>>::new(&mut reader, CsvProcessor::new(format.chars))
                .boxed_local();
        if let Some(max_records) = max_records {
            stream = stream.take(max_records).boxed_local();
        }
        let record_size = stream
            .try_fold(None, |last_size, values| {
                futures::future::ready({
                    let this_size = values.item.len();
                    if this_size < 2 || last_size.unwrap_or(this_size) != this_size {
                        Err(io::Error::other("Size mismatch"))
                    } else {
                        Ok(Some(this_size))
                    }
                })
            })
            .await;
        reader.rewind().await?;
        if record_size.is_ok_and(|size| size.is_some()) {
            best_format_guess = Some(format);
            break;
        }
    }
    let mut format =
        best_format_guess.ok_or_else(|| io::Error::other("Could not find a format"))?;
    let mut stream =
        CsvReadStream::<_, Vec<Value>>::new(&mut reader, CsvProcessor::new(format.chars));
    let first = stream
        .next()
        .await
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "First record not found"))??;
    if first.item.iter().all(|v| matches!(v, Value::String(_))) {
        format.has_headers = true;
    } else if let Some(second) = stream.next().await.transpose()? {
        let num_first_strings = first
            .item
            .iter()
            .filter(|v| matches!(v, Value::String(_)))
            .count();
        let num_second_strings = second
            .item
            .iter()
            .filter(|v| matches!(v, Value::String(_)))
            .count();
        if num_first_strings > num_second_strings {
            format.has_headers = true;
        }
    }
    Ok(format)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{format::AsyncFileReader, read::ValueStream, FormatReader};

    pub fn load_csv(path: &str, format: CsvFormat) -> FormatReader<impl AsyncFileReader> {
        FormatReader::new(
            std::io::Cursor::new(
                std::fs::read(format!("tests/data/files/csv/{path}.csv")).unwrap(),
            ),
            format.into(),
        )
    }

    #[tokio::test]
    async fn basic_example() {
        println!(
            "basic_example: {:#?}",
            load_csv(
                "example",
                CsvFormat {
                    has_headers: true,
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
    async fn example_no_headers() {
        println!(
            "example_no_headers: {:#?}",
            load_csv(
                "example_no_headers",
                CsvFormat {
                    has_headers: false,
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
    async fn null_test() {
        println!(
            "null_test: {:#?}",
            load_csv(
                "null_test",
                CsvFormat {
                    has_headers: true,
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
