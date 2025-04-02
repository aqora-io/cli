use wasm_bindgen::prelude::*;

use crate::error::Error;

impl From<JsError> for Error {
    fn from(err: JsError) -> Self {
        Error::Js(err.into())
    }
}

impl From<serde_wasm_bindgen::Error> for Error {
    fn from(err: serde_wasm_bindgen::Error) -> Self {
        Error::Js(err.into())
    }
}

impl From<JsValue> for Error {
    fn from(err: JsValue) -> Self {
        Error::Js(err)
    }
}

impl From<Error> for JsValue {
    fn from(err: Error) -> Self {
        match err {
            Error::Js(err) => err,
            err => JsError::from(err).into(),
        }
    }
}
