use wasm_bindgen_test::*;

use aqora_data_utils::wasm::{
    aqora_client::{
        checksum::{JsChecksum, S3ChecksumOptions},
        client::JsClientLayer,
    },
    error::set_console_error_panic_hook,
};

use super::utils::check_serde;

#[wasm_bindgen_test]
pub fn test_client_layer_serde() {
    set_console_error_panic_hook();
    let layer = JsClientLayer::Retry(Default::default());
    check_serde(&layer);
    let layer = JsClientLayer::S3Checksum(S3ChecksumOptions {
        algo: JsChecksum::Crc32,
    });
    check_serde(&layer);
}
