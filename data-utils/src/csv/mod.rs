pub mod reader;
pub use reader::{CsvProcessor, CsvReadStream};

use csv_core::{Reader, ReaderBuilder, Terminator};
use futures::prelude::*;
use regex::Regex;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::value::{DateParseOptions, Value, ValueExt};
use crate::Format;

#[derive(Debug, Clone)]
pub struct CsvFormat {
    pub has_headers: bool,
    pub delimiter: u8,
    /// None is CLRF
    pub terminator: Option<u8>,
    pub quote: u8,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub null_regex: Option<Regex>,
    pub true_regex: Option<Regex>,
    pub false_regex: Option<Regex>,
    pub date_parse: DateParseOptions,
}

impl CsvFormat {
    #[inline]
    pub(crate) fn check_null(&self, s: &str) -> bool {
        match self.null_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => s.is_empty(),
        }
    }
    #[inline]
    pub(crate) fn check_true(&self, s: &str) -> bool {
        match self.null_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => matches!(s, "true" | "True" | "TRUE"),
        }
    }
    #[inline]
    pub(crate) fn check_false(&self, s: &str) -> bool {
        match self.null_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => matches!(s, "false" | "False" | "FALSE"),
        }
    }
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            has_headers: false,
            terminator: None,
            delimiter: b',',
            quote: b'"',
            escape: None,
            comment: None,
            null_regex: None,
            true_regex: None,
            false_regex: None,
            date_parse: DateParseOptions::default(),
        }
    }
}

impl From<&CsvFormat> for Reader {
    fn from(value: &CsvFormat) -> Self {
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

impl From<CsvFormat> for Format {
    fn from(value: CsvFormat) -> Self {
        Self::Csv(value)
    }
}

pub async fn read<'a, R>(
    mut reader: R,
    options: CsvFormat,
) -> io::Result<impl Stream<Item = io::Result<Value>> + 'a>
where
    R: AsyncRead + AsyncSeek + Unpin + 'a,
{
    let headers = if options.has_headers {
        let headers =
            CsvReadStream::<_, Vec<String>>::new(&mut reader, CsvProcessor::new(options.clone()))
                .try_next()
                .await?;
        reader.rewind().await?;
        headers
    } else {
        None
    };
    let has_headers = headers.is_some();
    let mut stream =
        CsvReadStream::<_, Vec<Value>>::new(reader, CsvProcessor::new(options.clone()))
            .map(move |seq| Ok(Value::Seq(seq?).with_headers(headers.clone())?))
            .map_ok(move |v| {
                v.map_values(|v| match v {
                    Value::String(s) => {
                        if options.check_null(&s) {
                            Value::Unit
                        } else if options.check_true(&s) {
                            Value::Bool(true)
                        } else if options.check_false(&s) {
                            Value::Bool(false)
                        } else {
                            Value::String(options.date_parse.normalize(s))
                        }
                    }
                    _ => v,
                })
            })
            .boxed_local();
    if has_headers {
        stream = stream.skip(1).boxed_local();
    }
    Ok(stream)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{format::AsyncFileReader, infer, FormatReader};

    pub fn load_csv(path: &str, format: CsvFormat) -> FormatReader<impl AsyncFileReader> {
        FormatReader::new(
            std::io::Cursor::new(std::fs::read(format!("tests/data/csv/{path}.csv")).unwrap()),
            format.into(),
        )
    }

    fn infer_options() -> infer::Options {
        infer::Options::new().coerce_numbers(true)
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
            .infer_schema(infer_options(), None)
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
            .infer_schema(infer_options(), None)
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
            .infer_schema(infer_options(), None)
            .await
            .unwrap()
        );
    }
}
