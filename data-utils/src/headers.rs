use std::{fmt, io};

use thiserror::Error;

use crate::value::{Key, Value};

#[derive(Error, Debug)]
pub enum WithHeadersError<S, B>
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
{
    #[error("Expected Struct or List, received: {0:?}")]
    UnexpectedType(Value<S, B>),
    #[error("Headers do not match items: {0:?}")]
    HeaderLengthMismatch(Vec<Value<S, B>>),
}

impl<S, B> From<WithHeadersError<S, B>> for io::Error
where
    S: AsRef<str> + Send + Sync + fmt::Debug + 'static,
    B: AsRef<[u8]> + Send + Sync + fmt::Debug + 'static,
{
    fn from(value: WithHeadersError<S, B>) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value)
    }
}

impl<S, B> Value<S, B>
where
    S: AsRef<str> + From<String>,
    B: AsRef<[u8]>,
{
    pub fn with_headers<I, H>(self, headers: Option<I>) -> Result<Self, WithHeadersError<S, B>>
    where
        I: IntoIterator<Item = H>,
        H: Into<S>,
    {
        match self {
            Value::Map(map) => Ok(Value::Map(map)),
            Value::Seq(list) => {
                let headers: Vec<Key<S>> = headers
                    .map(|h| h.into_iter().map(|h| Key::new(h.into())).collect())
                    .unwrap_or_else(|| {
                        (0..list.len())
                            .map(|s| Key::new(format!("item{s}").into()))
                            .collect()
                    });
                if headers.len() < list.len() {
                    Err(WithHeadersError::HeaderLengthMismatch(list))
                } else {
                    let extra_values_n = headers.len() - list.len();
                    Ok(Value::Map(
                        headers
                            .into_iter()
                            .zip(
                                list.into_iter()
                                    .chain((0..extra_values_n).map(|_| Value::Null)),
                            )
                            .collect(),
                    ))
                }
            }
            ty => Err(WithHeadersError::UnexpectedType(ty)),
        }
    }
}
