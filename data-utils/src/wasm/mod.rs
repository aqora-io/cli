pub mod utils;

use tokio::io::AsyncReadExt;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn test_utils(stream: web_sys::ReadableStream) -> js_sys::Promise {
    utils::set_console_error_panic_hook();
    wasm_bindgen_futures::future_to_promise(async move {
        web_sys::console::log_1(&"test_utils".into());
        let mut reader = utils::JsAsyncReader::new(stream.values());
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| e.to_string())?;
        web_sys::console::log_1(&format!("{buffer:?}").into());
        Ok(JsValue::null())
    })
}
