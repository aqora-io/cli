use std::io;

use chrono::{format::StrftimeItems, DateTime, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use ron::{Map, Value};

const NAIVE_DATE_TIME_FMT: StrftimeItems<'static> = StrftimeItems::new("%Y-%m-%dT%H:%M:%S");
const NAIVE_DATE_FMT: StrftimeItems<'static> = StrftimeItems::new("%Y-%m-%d");

#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(export))]
pub struct DateParseOptions {
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub date_fmt: Option<String>,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub timestamp_fmt: Option<String>,
}

impl DateParseOptions {
    pub fn is_empty(&self) -> bool {
        self.date_fmt.is_none() && self.timestamp_fmt.is_none()
    }

    pub(crate) fn normalize(&self, s: String) -> String {
        if let Some(fmt) = self.timestamp_fmt.as_ref() {
            if let Ok(dt) = DateTime::parse_from_str(&s, fmt) {
                return dt.to_utc().to_rfc3339();
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(&s, fmt) {
                return dt.format_with_items(NAIVE_DATE_TIME_FMT).to_string();
            }
        }
        if let Some(fmt) = self.date_fmt.as_ref() {
            if let Ok(d) = NaiveDate::parse_from_str(&s, fmt) {
                return d.format_with_items(NAIVE_DATE_FMT).to_string();
            }
        }
        s
    }
}

#[derive(Error, Debug)]
pub enum WithHeadersError {
    #[error("Expected Struct or List, received: {0:?}")]
    UnexpectedType(Value),
    #[error("Headers do not match items: {0:?}")]
    HeaderLengthMismatch(Vec<Value>),
}

impl From<WithHeadersError> for io::Error {
    fn from(value: WithHeadersError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value)
    }
}

#[derive(Error, Debug)]
pub enum WithEntryError {
    #[error("Expected Struct, received: {0:?}")]
    UnexpectedType(Value),
}

impl From<WithEntryError> for io::Error {
    fn from(value: WithEntryError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value)
    }
}

pub trait ValueExt: Sized {
    fn map_values<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self;
    fn with_headers<I, H>(self, headers: Option<I>) -> Result<Self, WithHeadersError>
    where
        I: IntoIterator<Item = H>,
        H: Into<String>;
    fn with_entry(
        self,
        key: impl Into<Value>,
        value: impl Into<Value>,
    ) -> Result<Value, WithEntryError>;
}

impl ValueExt for Value {
    fn map_values<F>(self, f: F) -> Value
    where
        F: Fn(Value) -> Value,
    {
        match self {
            Value::Seq(s) => Value::Seq(s.into_iter().map(f).collect()),
            Value::Map(m) => Value::Map(m.into_iter().map(|(k, v)| (k, f(v))).collect()),
            value => f(value),
        }
    }

    fn with_headers<I, H>(self, headers: Option<I>) -> Result<Value, WithHeadersError>
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
    {
        match self {
            Value::Map(map) => Ok(Value::Map(map)),
            Value::Seq(list) => {
                let headers: Vec<String> = headers
                    .map(|h| h.into_iter().map(|h| h.into()).collect())
                    .unwrap_or_else(|| (0..list.len()).map(|s| format!("item{s}")).collect());
                if headers.len() < list.len() {
                    Err(WithHeadersError::HeaderLengthMismatch(list))
                } else {
                    let extra_values_n = headers.len() - list.len();
                    Ok(Value::Map(
                        headers
                            .into_iter()
                            .zip(
                                list.into_iter()
                                    .chain((0..extra_values_n).map(|_| Value::Unit)),
                            )
                            .collect(),
                    ))
                }
            }
            ty => Err(WithHeadersError::UnexpectedType(ty)),
        }
    }

    fn with_entry(
        self,
        key: impl Into<Value>,
        value: impl Into<Value>,
    ) -> Result<Value, WithEntryError> {
        match self {
            Value::Map(mut map) => {
                map.insert(key, value);
                Ok(Value::Map(map))
            }
            ty => Err(WithEntryError::UnexpectedType(ty)),
        }
    }
}
