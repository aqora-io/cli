use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use super::regex::{js_to_rust_regex, rust_to_js_regex};
use crate::csv::CsvFormat;
use crate::value::DateParseOptions;

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[ts(rename = "CsvFormat")]
#[ts(export, export_to = "bindings.ts")]
pub struct JsCsvFormat {
    pub has_headers: bool,
    pub delimiter: char,
    #[ts(optional)]
    pub terminator: Option<char>,
    pub quote: char,
    #[ts(optional)]
    pub escape: Option<char>,
    #[ts(optional)]
    pub comment: Option<char>,
    #[serde(with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub null_regex: Option<JsValue>,
    #[serde(with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub true_regex: Option<JsValue>,
    #[serde(with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub false_regex: Option<JsValue>,
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
