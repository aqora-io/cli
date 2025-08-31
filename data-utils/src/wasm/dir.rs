use std::collections::HashMap;
use std::path::PathBuf;

use futures::prelude::*;
use tokio::io::AsyncReadExt;
use wasm_bindgen::prelude::*;

use crate::dir::DirReaderOptions;
use crate::error::{Error, Result};
use crate::process::ProcessItemStream;
use crate::read::{self, ValueStream};
use crate::schema::SerdeSchema;
use crate::value::Value;

use super::cast::JsCastExt;
use super::error::WasmError;
use super::format::{InferAndStreamRecordBatchesOptions, InferSchemaOptions};
use super::io::AsyncBlobReader;
use super::iter::async_iterable;
use super::read::JsRecordBatchStream;
use super::serde::{from_value, to_value};

#[wasm_bindgen(js_name = DirReader)]
#[derive(Clone)]
pub struct JsDirReader {
    files: JsValue,
    options: JsValue,
}

impl JsDirReader {
    pub fn parse_files(&self) -> Result<HashMap<String, web_sys::Blob>, WasmError> {
        js_sys::Object::entries(&self.files.clone().cast_into::<js_sys::Object>()?)
            .iter()
            .map(|item| {
                let tuple = item.cast_into::<js_sys::Array>()?;
                let key: String = tuple.get(0).cast_into::<js_sys::JsString>()?.into();
                let value: web_sys::Blob = tuple.get(1).cast_into()?;
                Ok((key, value))
            })
            .collect()
    }

    pub fn parse_options(&self) -> Result<DirReaderOptions, WasmError> {
        Ok(from_value(self.options.clone())?)
    }

    pub fn stream_values(&self) -> Result<ProcessItemStream<'static, Value, Error>> {
        let files = self.parse_files()?;
        Ok(self.parse_options()?.stream_values(
            files.clone().into_keys().map(PathBuf::from),
            move |path| {
                let files = files.clone();
                async move {
                    let path = path.as_os_str().to_str().ok_or_else(|| {
                        JsError::new(&format!(
                            "Path contains invalid UTF-8 characters: {}",
                            path.display()
                        ))
                    })?;
                    let file = files
                        .get(path)
                        .ok_or_else(|| JsError::new(&format!("{path} not found in file list")))?;
                    let mut out = Vec::new();
                    AsyncBlobReader::new(file.clone())
                        .read_to_end(&mut out)
                        .await?;
                    Result::<_, Error>::Ok(out)
                }
            },
        ))
    }
}

#[wasm_bindgen(js_class = DirReader)]
impl JsDirReader {
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "Record<string, Blob>")] files: JsValue,
        #[wasm_bindgen(unchecked_param_type = "undefined | DirReaderOptions | string | null")]
        options: JsValue,
    ) -> Result<Self> {
        let options = if options.is_null() || options.is_undefined() {
            to_value(&DirReaderOptions::default())?
        } else if let Some(glob) = options.dyn_ref::<js_sys::JsString>() {
            to_value(&DirReaderOptions::new(String::from(glob))?)?
        } else {
            options
        };
        Ok(Self { files, options })
    }

    #[wasm_bindgen(getter, unchecked_return_type = "Record<string, Blob>")]
    pub fn files(&self) -> JsValue {
        self.files.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_files(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Record<string, Blob>")] files: JsValue,
    ) {
        self.files = files;
    }

    #[wasm_bindgen(getter, unchecked_return_type = " DirReaderOptions")]
    pub fn options(&self) -> JsValue {
        self.options.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_options(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "DirReaderOptions")] options: JsValue,
    ) {
        self.options = options;
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
                .stream_values()?
                .infer_schema(options.options, options.sample_size)
                .await?,
        )?)
    }

    #[wasm_bindgen(js_name = "streamValues", unchecked_return_type = "AsyncIterable<any>")]
    pub fn js_stream_values(&self) -> Result<js_sys::Object> {
        async_iterable(
            self.stream_values()?
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
            .stream_values()?
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
            .stream_values()?
            .into_inferred_record_batch_stream(
                options.infer_options,
                options.sample_size,
                options.read_options,
            )
            .await?
            .into())
    }
}
