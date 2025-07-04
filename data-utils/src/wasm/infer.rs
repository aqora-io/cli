use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use super::format::{InferSchemaOptions, JsFormatReader};
use super::serde::{from_value, to_value};
use crate::error::Result;
use crate::format::take_samples;
use crate::infer;
use crate::schema::SerdeSchema;

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DebugInferSchemaError {
    record_num: usize,
    byte_start: usize,
    byte_end: usize,
    #[ts(type = "object")]
    record: serde_json::Value,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DebugInferSchemaResult {
    schema: Option<SerdeSchema>,
    error: Option<DebugInferSchemaError>,
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
        let options = from_value::<Option<InferSchemaOptions>>(options)?.unwrap_or_default();
        let samples = take_samples(
            &mut self.as_rust()?.stream_values().await?,
            options.sample_size,
        )
        .await?;
        let result = if let Ok(schema) = infer::from_samples(&samples, options.options.clone()) {
            DebugInferSchemaResult {
                schema: Some(schema.into()),
                error: None,
            }
        } else {
            let debugged = infer::debug_samples(&samples, options.options);
            DebugInferSchemaResult {
                schema: debugged.schema.map(|schema| schema.into()),
                error: debugged.error.map(|(record_num, error)| {
                    let item = &samples[record_num];
                    DebugInferSchemaError {
                        record_num,
                        byte_start: item.start,
                        byte_end: item.end,
                        record: serde_json::to_value(&item.item).unwrap_or_default(),
                        message: error.to_string(),
                    }
                }),
            }
        };
        Ok(to_value(&result)?)
    }
}
