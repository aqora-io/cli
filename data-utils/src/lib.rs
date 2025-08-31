#[cfg(feature = "aqora-client")]
pub mod aqora_client;
mod async_util;
#[cfg(feature = "csv")]
pub mod csv;
pub mod dir;
pub mod error;
#[cfg(any(feature = "csv", feature = "json"))]
pub mod format;
#[cfg(feature = "fs")]
pub mod fs;
pub mod infer;
#[cfg(feature = "ipc")]
pub mod ipc;
#[cfg(feature = "json")]
pub mod json;
mod process;
pub mod read;
pub mod schema;
mod serde;
pub mod utils;
pub mod value;
#[cfg(feature = "wasm")]
pub mod wasm;
pub mod write;

pub use arrow;
pub use parquet;

#[cfg(feature = "csv")]
pub use csv::CsvFormat;
pub use error::{Error, Result};
#[cfg(any(feature = "csv", feature = "json"))]
pub use format::{Format, FormatReader};
#[cfg(feature = "json")]
pub use json::JsonFormat;
pub use process::ProcessItem;
pub use read::ValueStream;
pub use schema::Schema;
pub use value::DateParseOptions;
