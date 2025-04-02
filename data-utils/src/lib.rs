#[cfg(feature = "csv")]
pub mod csv;
pub mod error;
pub mod format;
#[cfg(feature = "fs")]
pub mod fs;
pub mod infer;
#[cfg(feature = "json")]
pub mod json;
mod process;
pub mod read;
pub mod schema;
mod serde;
pub mod value;
#[cfg(feature = "wasm")]
pub mod wasm;
pub mod write;

pub use arrow;
pub use parquet;

#[cfg(feature = "csv")]
pub use csv::CsvFormat;
pub use error::{Error, Result};
pub use format::{Format, FormatReader};
#[cfg(feature = "json")]
pub use json::JsonFormat;
pub use process::ProcessItem;
pub use schema::Schema;
pub use value::DateParseOptions;
