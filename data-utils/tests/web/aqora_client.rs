use std::time::Duration;

use futures::future::{select, Either};
use wasm_bindgen_test::*;

use aqora_client::{retry::ExponentialBackoffBuilder, sleep::sleep};
use aqora_data_utils::wasm::{
    aqora_client::{
        checksum::{JsChecksum, S3ChecksumOptions},
        client::JsClientLayer,
        retry::JsBackoff,
    },
    error::set_console_error_panic_hook,
    serde::from_value,
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

#[wasm_bindgen_test]
pub fn test_exponential_backoff_options_fall_back_to_builder_defaults() {
    let value =
        js_sys::JSON::parse(r#"{"kind":"exponential","start_delay_ms":100,"factor":2}"#).unwrap();
    let JsBackoff::Exponential(options) = from_value(value).unwrap();
    let builder = ExponentialBackoffBuilder::from(options);
    let defaults = ExponentialBackoffBuilder::default();
    assert_eq!(builder.start_delay, Duration::from_millis(100));
    assert_eq!(builder.max_delay, defaults.max_delay);
    assert_eq!(builder.max_retries, defaults.max_retries);
}

#[wasm_bindgen_test]
pub async fn test_sleep_waits_for_the_duration() {
    let start = js_sys::Date::now();
    sleep(Duration::from_millis(100)).await;
    assert!(js_sys::Date::now() - start >= 90.);
}

#[wasm_bindgen_test]
pub async fn test_sleep_saturates_durations_beyond_i32_millis() {
    let long = sleep(Duration::from_millis(1 << 31));
    let short = sleep(Duration::from_millis(50));
    assert!(matches!(select(long, short).await, Either::Right(_)));
}
