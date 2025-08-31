use wasm_bindgen::prelude::*;

use crate::error::Result;

use super::io::AsyncBlobReader;

#[wasm_bindgen(js_name = "isParquet")]
pub async fn is_parquet(blob: web_sys::Blob) -> Result<bool> {
    Ok(crate::utils::is_parquet(&mut AsyncBlobReader::new(blob)).await?)
}
