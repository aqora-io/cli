use wasm_bindgen::prelude::*;

use crate::error::Result;
use crate::format::ValueStream;
use crate::read::RecordBatchStream;

use super::serde::to_value;

#[wasm_bindgen(js_name = "RecordBatchStream")]
pub struct JsRecordBatchStream(#[wasm_bindgen(skip)] pub RecordBatchStream<ValueStream<'static>>);

#[wasm_bindgen(js_class = "RecordBatchStream")]
impl JsRecordBatchStream {
    #[wasm_bindgen(unchecked_return_type = "bindings.Schema")]
    pub async fn schema(&self) -> Result<JsValue> {
        Ok(to_value(self.0.schema())?)
    }
}

impl From<RecordBatchStream<ValueStream<'static>>> for JsRecordBatchStream {
    fn from(value: RecordBatchStream<ValueStream<'static>>) -> Self {
        JsRecordBatchStream(value)
    }
}
