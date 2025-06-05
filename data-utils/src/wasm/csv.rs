use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use super::regex::{js_to_rust_regex, rust_to_js_regex};
use crate::csv::{CsvFormat, CsvFormatChars, CsvFormatRegex};
use crate::value::DateParseOptions;

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(export, rename = "CsvFormatRegex")]
pub struct JsCsvFormatRegex {
    #[serde(rename = "null", default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub null_regex: Option<JsValue>,
    #[serde(rename = "true", default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub true_regex: Option<JsValue>,
    #[serde(rename = "false", default, with = "super::serde::preserve::option")]
    #[ts(optional, type = "RegExp | string")]
    pub false_regex: Option<JsValue>,
}

impl TryFrom<JsCsvFormatRegex> for CsvFormatRegex {
    type Error = JsError;
    fn try_from(value: JsCsvFormatRegex) -> Result<Self, Self::Error> {
        Ok(Self {
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
        })
    }
}

impl TryFrom<CsvFormatRegex> for JsCsvFormatRegex {
    type Error = JsError;
    fn try_from(value: CsvFormatRegex) -> Result<Self, Self::Error> {
        Ok(Self {
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
        })
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(rename = "CsvFormat", export)]
pub struct JsCsvFormat {
    #[serde(default)]
    #[ts(optional, as = "Option<bool>")]
    pub has_headers: bool,
    #[serde(default)]
    pub chars: CsvFormatChars,
    #[serde(default)]
    #[ts(optional, as = "Option<JsCsvFormatRegex>")]
    pub regex: JsCsvFormatRegex,
    #[serde(default)]
    #[ts(optional, as = "Option<DateParseOptions>")]
    pub date: DateParseOptions,
}

impl TryFrom<JsCsvFormat> for CsvFormat {
    type Error = JsError;
    fn try_from(value: JsCsvFormat) -> Result<Self, Self::Error> {
        Ok(Self {
            has_headers: value.has_headers,
            chars: value.chars,
            regex: value.regex.try_into()?,
            date: value.date,
        })
    }
}

impl TryFrom<CsvFormat> for JsCsvFormat {
    type Error = JsError;
    fn try_from(value: CsvFormat) -> Result<Self, Self::Error> {
        Ok(Self {
            has_headers: value.has_headers,
            chars: value.chars,
            regex: value.regex.try_into()?,
            date: value.date,
        })
    }
}
