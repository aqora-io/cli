use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use super::regex::{js_to_rust_regex, rust_to_js_regex};
use crate::csv::{CsvFormat, DEFAULT_DELIMITER, DEFAULT_QUOTE};
use crate::value::DateParseOptions;

const fn default_delimiter() -> char {
    DEFAULT_DELIMITER as char
}

const fn default_quote() -> char {
    DEFAULT_QUOTE as char
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[ts(rename = "CsvFormat", export, export_to = "bindings.ts")]
pub struct JsCsvFormat {
    #[serde(default)]
    #[ts(optional, as = "Option<bool>")]
    pub has_headers: bool,
    #[serde(default = "default_delimiter")]
    #[ts(optional, as = "Option<char>")]
    pub delimiter: char,
    #[serde(default)]
    #[ts(optional)]
    pub terminator: Option<char>,
    #[serde(default = "default_quote")]
    #[ts(optional, as = "Option<char>")]
    pub quote: char,
    #[serde(default)]
    #[ts(optional)]
    pub escape: Option<char>,
    #[serde(default)]
    #[ts(optional)]
    pub comment: Option<char>,
    #[serde(default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub null_regex: Option<JsValue>,
    #[serde(default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub true_regex: Option<JsValue>,
    #[serde(default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub false_regex: Option<JsValue>,
    #[serde(default)]
    #[ts(optional, as = "Option<DateParseOptions>")]
    pub date_parse: DateParseOptions,
}

impl Default for JsCsvFormat {
    fn default() -> Self {
        CsvFormat::default()
            .try_into()
            .expect("Default CSV format should be convertible to JS")
    }
}

impl TryFrom<JsCsvFormat> for CsvFormat {
    type Error = JsError;
    fn try_from(value: JsCsvFormat) -> Result<Self, Self::Error> {
        Ok(Self {
            has_headers: value.has_headers,
            delimiter: value.delimiter.try_into()?,
            terminator: value.terminator.map(TryFrom::try_from).transpose()?,
            quote: value.quote.try_into()?,
            escape: value.escape.map(TryFrom::try_from).transpose()?,
            comment: value.comment.map(TryFrom::try_from).transpose()?,
            null_regex: value
                .null_regex
                .as_ref()
                .map(js_to_rust_regex)
                .transpose()?,
            true_regex: value
                .true_regex
                .as_ref()
                .map(js_to_rust_regex)
                .transpose()?,
            false_regex: value
                .false_regex
                .as_ref()
                .map(js_to_rust_regex)
                .transpose()?,
            date_parse: value.date_parse,
        })
    }
}

impl TryFrom<CsvFormat> for JsCsvFormat {
    type Error = JsError;
    fn try_from(value: CsvFormat) -> Result<Self, Self::Error> {
        Ok(Self {
            has_headers: value.has_headers,
            delimiter: value.delimiter.into(),
            terminator: value.terminator.map(From::from),
            quote: value.quote.into(),
            escape: value.escape.map(From::from),
            comment: value.comment.map(From::from),
            null_regex: value
                .null_regex
                .as_ref()
                .map(rust_to_js_regex)
                .transpose()?,
            true_regex: value
                .true_regex
                .as_ref()
                .map(rust_to_js_regex)
                .transpose()?,
            false_regex: value
                .false_regex
                .as_ref()
                .map(rust_to_js_regex)
                .transpose()?,
            date_parse: value.date_parse,
        })
    }
}
