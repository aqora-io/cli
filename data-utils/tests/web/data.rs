use std::path::Path;

use aqora_data_utils::schema::SerdeSchema;
use aqora_data_utils::wasm::{format::JsFormat, serde::from_value};
use include_dir::{include_dir, Dir};
use wasm_bindgen::prelude::*;

pub static TEST_DATA: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/tests/data");

pub const JSON: &str = "json";
pub const CSV: &str = "csv";

pub fn data_files(format: &str) -> &'static Dir<'static> {
    TEST_DATA.get_dir(format!("files/{format}")).unwrap()
}

fn path_parts(test_file: &Path) -> (&str, &str) {
    let mut parts = test_file.iter().skip(1);
    let format = parts.next().unwrap().to_str().unwrap();
    let name = Path::new(parts.next().unwrap())
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap();
    (format, name)
}

pub enum DataSchema {
    Error,
    Schema(SerdeSchema),
}

pub fn data_config_for(test_file: impl AsRef<Path>) -> Option<JsFormat> {
    let (format, name) = path_parts(test_file.as_ref());
    let file = TEST_DATA.get_file(format!("config/{format}/{name}.json"))?;
    Some(from_value(js_sys::JSON::parse(file.contents_utf8().unwrap()).unwrap()).unwrap())
}

pub fn data_schema_for(test_file: impl AsRef<Path>) -> DataSchema {
    let (format, name) = path_parts(test_file.as_ref());
    let file = TEST_DATA
        .get_file(format!("schema/{format}/{name}.json"))
        .unwrap();
    let object = js_sys::JSON::parse(file.contents_utf8().unwrap()).unwrap();
    if js_sys::Reflect::has(object.as_ref(), &JsValue::from("error")).unwrap() {
        DataSchema::Error
    } else {
        DataSchema::Schema(from_value(object).unwrap())
    }
}
