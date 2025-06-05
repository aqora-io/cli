#![allow(dead_code)]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_worker);

#[cfg(feature = "aqora-client")]
mod aqora_client;
mod data;
mod format;
mod io;
