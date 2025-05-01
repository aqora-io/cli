use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::format::FileKind;

use super::serde::DeserializeTagged;

#[derive(TS, Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "format", rename_all = "snake_case")]
#[ts(rename = "Format")]
#[ts(export, export_to = "bindings.ts")]
#[non_exhaustive]
pub enum JsFormat {
    #[cfg(feature = "csv")]
    Csv(super::csv::JsCsvFormat),
    #[cfg(feature = "json")]
    Json(crate::json::JsonFormat),
}

impl<'de> DeserializeTagged<'de> for JsFormat {
    const TAG: &'static str = "format";
    type Tag = FileKind;

    fn deserialize_tagged<D>(tag: Self::Tag, deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(match tag {
            #[cfg(feature = "csv")]
            FileKind::Csv => JsFormat::Csv(super::csv::JsCsvFormat::deserialize(deserializer)?),
            #[cfg(feature = "json")]
            FileKind::Json => JsFormat::Json(crate::json::JsonFormat::deserialize(deserializer)?),
        })
    }
}

impl<'de> Deserialize<'de> for JsFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Self as DeserializeTagged>::deserialize(deserializer)
    }
}

#[wasm_bindgen(js_name = FormatReader)]
pub struct JsFormatReader {
    reader: web_sys::Blob,
    format: JsValue,
}

#[wasm_bindgen(js_class = FormatReader)]
impl JsFormatReader {
    #[wasm_bindgen(constructor)]
    pub fn new(
        reader: web_sys::Blob,
        #[wasm_bindgen(unchecked_param_type = "bindings.Format")] format: JsValue,
    ) -> Self {
        Self { reader, format }
    }

    #[wasm_bindgen(getter)]
    pub fn reader(&self) -> web_sys::Blob {
        self.reader.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_reader(&mut self, reader: web_sys::Blob) {
        self.reader = reader;
    }

    #[wasm_bindgen(getter, unchecked_return_type = "bindings.Format")]
    pub fn format(&self) -> JsValue {
        self.format.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_format(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "bindings.Format")] format: JsValue,
    ) {
        self.format = format;
    }
}
