pub mod reader;
pub use reader::{CsvProcessor, CsvReadStream};

use csv_core::{Reader, ReaderBuilder, Terminator};
use futures::prelude::*;
use regex::Regex;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::process::ProcessItem;
use crate::value::{DateParseOptions, Value, ValueExt};
use crate::Format;

pub const DEFAULT_DELIMITER: u8 = b',';
pub const DEFAULT_QUOTE: u8 = b'"';

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

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            has_headers: false,
            terminator: None,
            delimiter: DEFAULT_DELIMITER,
            quote: DEFAULT_QUOTE,
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
) -> io::Result<impl Stream<Item = io::Result<ProcessItem<Value>>> + 'a>
where
    R: AsyncRead + AsyncSeek + Unpin + 'a,
{
    let headers = if options.has_headers {
        let headers =
            CsvReadStream::<_, Vec<String>>::new(&mut reader, CsvProcessor::new(options.clone()))
                .map_ok(|item| item.item)
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
            .map(move |item| {
                Ok(item?
                    .map(|seq| Value::Seq(seq).with_headers(headers.clone()))
                    .transpose()?)
            })
            .map_ok(move |item| {
                item.map(|v| {
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
            })
            .boxed_local();
    if has_headers {
        stream = stream.skip(1).boxed_local();
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
            delimiter,
            quote,
            escape,
            ..Default::default()
        });
    let mut best_format_guess = None;
    for format in format_guesses {
        let mut stream =
            CsvReadStream::<_, Vec<Value>>::new(&mut reader, CsvProcessor::new(format.clone()))
                .boxed_local();
        if let Some(max_records) = max_records {
            stream = stream.take(max_records).boxed_local();
        }
        let record_size = stream
            .try_fold(None, |last_size, values| {
                futures::future::ready({
                    let this_size = values.item.len();
                    if this_size < 2 || last_size.unwrap_or(this_size) != this_size {
                        Err(io::Error::new(io::ErrorKind::Other, "Size mismatch"))
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
    let mut format = best_format_guess
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Could not find a format"))?;
    let mut stream =
        CsvReadStream::<_, Vec<Value>>::new(&mut reader, CsvProcessor::new(format.clone()));
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
    use crate::{format::AsyncFileReader, FormatReader};

    pub fn load_csv(path: &str, format: CsvFormat) -> FormatReader<impl AsyncFileReader> {
        FormatReader::new(
            std::io::Cursor::new(std::fs::read(format!("tests/data/csv/{path}.csv")).unwrap()),
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
            .infer_schema(Default::default(), None)
            .await
            .unwrap()
        );
    }
}
