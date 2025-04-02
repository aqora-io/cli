pub mod blob;
#[cfg(feature = "csv")]
pub mod csv;
pub mod error;
pub mod format;
pub mod io;
pub mod iter;
pub mod read;
pub mod regex;
pub mod serde;
pub mod write;

use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &str = include_str!("./bindings.ts");
