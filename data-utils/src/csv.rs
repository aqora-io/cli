use arrow::csv::reader::Format as ArrowFormat;
use csv_async::{AsyncReaderBuilder, Terminator};
use regex::Regex;

#[derive(Debug, Clone)]
pub struct Format {
    pub has_headers: bool,
    pub delimiter: u8,
    /// None is CLRF
    pub terminator: Option<u8>,
    pub quote: u8,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub truncate_rows: bool,
    pub null_regex: Option<Regex>,
}

impl Format {
    #[inline]
    pub(crate) fn check_null(&self, s: &str) -> bool {
        match self.null_regex.as_ref() {
            Some(r) => r.is_match(s),
            None => s.is_empty(),
        }
    }
}

impl Default for Format {
    fn default() -> Self {
        Self {
            has_headers: false,
            terminator: None,
            delimiter: b',',
            quote: b'"',
            escape: None,
            comment: None,
            truncate_rows: false,
            null_regex: None,
        }
    }
}

impl From<Format> for ArrowFormat {
    fn from(value: Format) -> Self {
        let mut fmt = ArrowFormat::default()
            .with_header(value.has_headers)
            .with_delimiter(value.delimiter)
            .with_quote(value.quote);
        if let Some(terminator) = value.terminator {
            fmt = fmt.with_terminator(terminator)
        }
        if let Some(escape) = value.escape {
            fmt = fmt.with_escape(escape)
        }
        if let Some(comment) = value.comment {
            fmt = fmt.with_comment(comment)
        }
        if let Some(null_regex) = value.null_regex {
            fmt = fmt.with_null_regex(null_regex)
        }
        if value.truncate_rows {
            fmt = fmt.with_truncated_rows(value.truncate_rows)
        }
        fmt
    }
}

impl From<Format> for AsyncReaderBuilder {
    fn from(value: Format) -> Self {
        let mut reader = AsyncReaderBuilder::default();
        reader
            .delimiter(value.delimiter)
            .has_headers(value.has_headers)
            .flexible(!value.truncate_rows)
            .quote(value.quote)
            .escape(value.escape)
            .comment(value.comment)
            .terminator(
                value
                    .terminator
                    .map(Terminator::Any)
                    .unwrap_or(Terminator::CRLF),
            );
        reader
    }
}
