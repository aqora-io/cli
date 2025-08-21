use futures::prelude::*;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::error::{Error, Result};
use crate::format::{FileKind, Format, FormatReader};
use crate::schema::SerdeSchema;
use crate::{
    infer,
    read::{self, ValueStream},
};

use super::io::AsyncBlobReader;
use super::iter::async_iterable;
use super::read::JsRecordBatchStream;
use super::serde::{from_value, to_value, DeserializeTagged};

#[derive(TS, Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "format", rename_all = "snake_case")]
#[ts(rename = "Format", export)]
#[non_exhaustive]
pub enum JsFormat {
    #[cfg(feature = "csv")]
    Csv(super::csv::JsCsvFormat),
    #[cfg(feature = "json")]
    Json(crate::json::JsonFormat),
}

impl TryFrom<Format> for JsFormat {
    type Error = JsError;
    fn try_from(value: Format) -> Result<Self, Self::Error> {
        Ok(match value {
            #[cfg(feature = "csv")]
            Format::Csv(format) => JsFormat::Csv(format.try_into()?),
            #[cfg(feature = "json")]
            Format::Json(format) => JsFormat::Json(format),
        })
    }
}

impl TryFrom<JsFormat> for Format {
    type Error = JsError;
    fn try_from(value: JsFormat) -> Result<Self, Self::Error> {
        Ok(match value {
            #[cfg(feature = "csv")]
            JsFormat::Csv(format) => Format::Csv(format.try_into()?),
            #[cfg(feature = "json")]
            JsFormat::Json(format) => Format::Json(format),
        })
    }
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

#[derive(TS, Serialize, Deserialize, Debug, Clone, Default)]
#[ts(export)]
pub struct InferSchemaOptions {
    #[serde(flatten)]
    pub options: infer::Options,
    #[serde(default)]
    #[ts(optional)]
    pub sample_size: Option<usize>,
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, Default)]
#[ts(export)]
pub struct InferAndStreamRecordBatchesOptions {
    #[serde(flatten)]
    pub infer_options: infer::Options,
    #[serde(default)]
    #[ts(optional)]
    pub sample_size: Option<usize>,
    #[serde(flatten)]
    pub read_options: read::Options,
}

#[wasm_bindgen(js_name = FormatReader)]
#[derive(Clone)]
pub struct JsFormatReader {
    reader: web_sys::Blob,
    format: JsValue,
}

impl JsFormatReader {
    pub fn as_rust(&self) -> Result<FormatReader<AsyncBlobReader>, JsError> {
        self.clone().try_into()
    }
}

#[wasm_bindgen(js_class = FormatReader)]
impl JsFormatReader {
    #[wasm_bindgen(constructor)]
    pub fn new(
        reader: web_sys::Blob,
        #[wasm_bindgen(unchecked_param_type = "Format")] format: JsValue,
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

    #[wasm_bindgen(getter, unchecked_return_type = "Format")]
    pub fn format(&self) -> JsValue {
        self.format.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_format(&mut self, #[wasm_bindgen(unchecked_param_type = "Format")] format: JsValue) {
        self.format = format;
    }

    #[wasm_bindgen(js_name = "inferSchema", unchecked_return_type = "Schema")]
    pub async fn infer_schema(
        &self,
        #[wasm_bindgen(unchecked_param_type = "undefined | InferSchemaOptions | null")]
        options: JsValue,
    ) -> Result<JsValue> {
        let options = from_value::<Option<InferSchemaOptions>>(options)?.unwrap_or_default();
        Ok(to_value(
            &self
                .as_rust()?
                .stream_values()
                .await?
                .infer_schema(options.options, options.sample_size)
                .await?,
        )?)
    }

    #[wasm_bindgen(js_name = "streamValues", unchecked_return_type = "AsyncIterable<any>")]
    pub async fn stream_values(&self) -> Result<js_sys::Object> {
        async_iterable(
            self.as_rust()?
                .into_value_stream()
                .await?
                .map_err(Error::from)
                .map(|res| res.and_then(|val| Ok(super::serde::to_value(&val)?))),
        )
    }

    #[wasm_bindgen(js_name = "streamRecordBatches")]
    pub async fn stream_record_batches(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Schema")] schema: JsValue,
        #[wasm_bindgen(unchecked_param_type = "undefined | ReadOptions | null")] options: JsValue,
    ) -> Result<JsRecordBatchStream> {
        let schema = from_value::<SerdeSchema>(schema)?.into();
        let options = from_value::<Option<read::Options>>(options)?.unwrap_or_default();
        Ok(self
            .as_rust()?
            .into_value_stream()
            .await?
            .into_record_batch_stream(schema, options)?
            .into())
    }

    #[wasm_bindgen(js_name = "inferAndStreamRecordBatches")]
    pub async fn infer_and_stream_record_batches(
        &self,
        #[wasm_bindgen(
            unchecked_param_type = "undefined | InferAndStreamRecordBatchesOptions | null"
        )]
        options: JsValue,
    ) -> Result<JsRecordBatchStream> {
        let options =
            from_value::<Option<InferAndStreamRecordBatchesOptions>>(options)?.unwrap_or_default();
        Ok(self
            .as_rust()?
            .into_value_stream()
            .await?
            .into_inferred_record_batch_stream(
                options.infer_options,
                options.sample_size,
                options.read_options,
            )
            .await?
            .into())
    }
}

impl TryFrom<JsFormatReader> for FormatReader<AsyncBlobReader> {
    type Error = JsError;
    fn try_from(value: JsFormatReader) -> Result<FormatReader<AsyncBlobReader>, JsError> {
        Ok(FormatReader::new(
            AsyncBlobReader::new(value.reader),
            from_value::<JsFormat>(value.format)?.try_into()?,
        ))
    }
}

impl TryFrom<FormatReader<AsyncBlobReader>> for JsFormatReader {
    type Error = JsError;
    fn try_from(value: FormatReader<AsyncBlobReader>) -> Result<JsFormatReader, JsError> {
        Ok(JsFormatReader::new(
            value.reader.into_inner(),
            to_value(&JsFormat::try_from(value.format)?)?,
        ))
    }
}
