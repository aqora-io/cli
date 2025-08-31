use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::error::{Error, Result};
use crate::infer;
use crate::process::ProcessItemStream;
use crate::schema::SerdeSchema;
use crate::value::Value;

use super::dir::JsDirReader;
use super::format::{InferSchemaOptions, JsFormatReader};
use super::serde::{from_value, to_value};

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DebugInferSchemaError {
    record_num: usize,
    byte_start: u64,
    byte_end: u64,
    #[ts(type = "object")]
    record: serde_json::Value,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DebugInferSchemaResult {
    schema: Option<SerdeSchema>,
    error: Option<DebugInferSchemaError>,
    samples_read: usize,
    bytes_read: u64,
}

async fn debug_infer_schema<E>(
    mut stream: ProcessItemStream<'static, Value, E>,
    options: JsValue,
) -> Result<JsValue>
where
    Error: From<E>,
{
    let options = from_value::<Option<InferSchemaOptions>>(options)?.unwrap_or_default();
    let samples = infer::take_samples(&mut stream, options.sample_size).await?;
    let (schema, error) = if let Ok(schema) = infer::from_samples(&samples, options.options.clone())
    {
        (Some(schema.into()), None)
    } else {
        let debugged = infer::debug_samples(&samples, options.options);
        let schema = debugged.schema.map(|schema| schema.into());
        let error = debugged.error.map(|(record_num, error)| {
            let item = &samples[record_num];
            DebugInferSchemaError {
                record_num,
                byte_start: item.start,
                byte_end: item.end,
                record: serde_json::to_value(&item.item).unwrap_or_default(),
                message: error.to_string(),
            }
        });
        (schema, error)
    };
    let samples_read = samples.len();
    let bytes_read = samples.last().map(|item| item.end).unwrap_or(0);
    Ok(to_value(&DebugInferSchemaResult {
        schema,
        error,
        samples_read,
        bytes_read,
    })?)
}

#[wasm_bindgen(js_class = FormatReader)]
impl JsFormatReader {
    #[wasm_bindgen(
        js_name = "debugInferSchema",
        unchecked_return_type = "DebugInferSchemaResult"
    )]
    pub async fn debug_infer_schema(
        &self,
        #[wasm_bindgen(unchecked_param_type = "undefined | InferSchemaOptions | null")]
        options: JsValue,
    ) -> Result<JsValue> {
        debug_infer_schema(self.as_rust()?.into_value_stream().await?, options).await
    }
}

#[wasm_bindgen(js_class = DirReader)]
impl JsDirReader {
    #[wasm_bindgen(
        js_name = "debugInferSchema",
        unchecked_return_type = "DebugInferSchemaResult"
    )]
    pub async fn debug_infer_schema(
        &self,
        #[wasm_bindgen(unchecked_param_type = "undefined | InferSchemaOptions | null")]
        options: JsValue,
    ) -> Result<JsValue> {
        debug_infer_schema(self.stream_values()?, options).await
    }
}
