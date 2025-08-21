use futures::prelude::*;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::error::{Error, Result};
use crate::process::ProcessItemStream;
use crate::read::{self, ValueRecordBatchStream};
use crate::schema::SerdeSchema;
use crate::value::Value;

use super::dir::JsDirReader;
use super::format::JsFormatReader;
use super::serde::{from_value, to_value};

#[wasm_bindgen(js_name = "RecordBatchStream")]
pub struct JsRecordBatchStream(
    #[wasm_bindgen(skip)] pub ValueRecordBatchStream<ProcessItemStream<'static, Value, Error>>,
);

#[wasm_bindgen(js_class = "RecordBatchStream")]
impl JsRecordBatchStream {
    #[wasm_bindgen(unchecked_return_type = "Schema")]
    pub async fn schema(&self) -> Result<JsValue> {
        Ok(to_value(self.0.schema())?)
    }
}

impl From<ValueRecordBatchStream<ProcessItemStream<'static, Value, Error>>>
    for JsRecordBatchStream
{
    fn from(value: ValueRecordBatchStream<ProcessItemStream<'static, Value, Error>>) -> Self {
        JsRecordBatchStream(value)
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, Default)]
#[ts(export)]
pub struct TestStreamRecordBatchesOptions {
    #[serde(default)]
    #[ts(optional)]
    pub sample_size: Option<usize>,
    #[serde(flatten)]
    pub read_options: read::Options,
}

async fn test_stream_record_batches<E>(
    mut stream: ProcessItemStream<'static, Value, E>,
    schema: JsValue,
    options: JsValue,
) -> Result<()>
where
    Error: From<E>,
{
    let options =
        from_value::<Option<TestStreamRecordBatchesOptions>>(options)?.unwrap_or_default();
    let schema = from_value::<SerdeSchema>(schema)?.into();
    if let Some(sample_size) = options.sample_size {
        stream = stream.take(sample_size).boxed_local()
    };
    let record_batches = read::from_value_stream(stream, schema, options.read_options)?;
    record_batches.try_all(|_| async move { true }).await?;
    Ok(())
}

#[wasm_bindgen(js_class = FormatReader)]
impl JsFormatReader {
    #[wasm_bindgen(js_name = "testStreamRecordBatches")]
    pub async fn test_stream_record_batches(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Schema")] schema: JsValue,
        #[wasm_bindgen(unchecked_param_type = "undefined | TestStreamRecordBatchesOptions | null")]
        options: JsValue,
    ) -> Result<()> {
        test_stream_record_batches(self.as_rust()?.into_value_stream().await?, schema, options)
            .await
    }
}

#[wasm_bindgen(js_class = DirReader)]
impl JsDirReader {
    #[wasm_bindgen(js_name = "testStreamRecordBatches")]
    pub async fn test_stream_record_batches(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Schema")] schema: JsValue,
        #[wasm_bindgen(unchecked_param_type = "undefined | TestStreamRecordBatchesOptions | null")]
        options: JsValue,
    ) -> Result<()> {
        test_stream_record_batches(self.stream_values()?, schema, options).await
    }
}
