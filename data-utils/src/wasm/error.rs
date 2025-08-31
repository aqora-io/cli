use thiserror::Error;
use wasm_bindgen::prelude::*;

use super::cast::JsCastExt;
use crate::error::Error;

#[cfg_attr(
    feature = "console_error_panic_hook",
    wasm_bindgen(js_name = setConsoleErrorPanicHook)
)]
pub fn set_console_error_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

#[derive(Debug, Error)]
pub enum WasmError {
    #[error(transparent)]
    Type(#[from] UnexpectedTypeError),
    #[error(transparent)]
    Call(#[from] JsCallError),
}

impl From<WasmError> for JsValue {
    fn from(value: WasmError) -> Self {
        match value {
            WasmError::Type(err) => JsError::from(err).into(),
            WasmError::Call(err) => err.into(),
        }
    }
}

impl From<WasmError> for std::io::Error {
    fn from(value: WasmError) -> Self {
        std::io::Error::other(value.to_string())
    }
}

impl From<JsValue> for WasmError {
    fn from(value: JsValue) -> Self {
        WasmError::Call(value.into())
    }
}

impl From<JsError> for WasmError {
    fn from(value: JsError) -> Self {
        WasmError::Call(value.into())
    }
}

impl From<serde_wasm_bindgen::Error> for WasmError {
    fn from(value: serde_wasm_bindgen::Error) -> Self {
        WasmError::Call(value.into())
    }
}

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
        Error::Js(err.into())
    }
}

impl From<UnexpectedTypeError> for Error {
    fn from(err: UnexpectedTypeError) -> Self {
        Error::Js(err.into())
    }
}

impl From<Error> for JsValue {
    fn from(err: Error) -> Self {
        match err {
            Error::Js(err) => err.into(),
            err => JsError::from(err).into(),
        }
    }
}

#[derive(Debug, Error)]
#[error("Expected {} received {}", .0, .1.type_name())]
pub struct UnexpectedTypeError(&'static str, JsValue);

impl UnexpectedTypeError {
    pub fn new<T>(value: JsValue) -> Self {
        Self(std::any::type_name::<T>(), value)
    }
}

impl UnexpectedTypeError {
    pub fn into_value(self) -> JsValue {
        self.1
    }
}

#[derive(Debug, Error)]
#[error("{}", .0.as_string().unwrap_or_else(|| "Unknown JS Error".to_string()))]
pub struct JsCallError(JsValue);

impl From<JsValue> for JsCallError {
    fn from(value: JsValue) -> Self {
        Self(value)
    }
}

impl From<JsError> for JsCallError {
    fn from(value: JsError) -> Self {
        Self(value.into())
    }
}

impl From<serde_wasm_bindgen::Error> for JsCallError {
    fn from(value: serde_wasm_bindgen::Error) -> Self {
        Self(value.into())
    }
}

impl From<JsCallError> for JsValue {
    fn from(value: JsCallError) -> Self {
        value.0
    }
}
