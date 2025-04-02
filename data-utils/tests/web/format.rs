use wasm_bindgen_test::*;

use aqora_data_utils::wasm::{format::JsFormat, io::set_console_error_panic_hook};

fn check_serde<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    use aqora_data_utils::wasm::serde::{from_value, to_value};
    assert_eq!(value, &from_value::<T>(to_value(&value).unwrap()).unwrap());
}

#[wasm_bindgen_test]
pub fn test_format_serde() {
    set_console_error_panic_hook();
    let default_json = JsFormat::Json(Default::default());
    check_serde(&default_json);
    let default_csv = JsFormat::Csv(Default::default());
    check_serde(&default_csv);

    let csv_format_custom_null = aqora_data_utils::csv::CsvFormat {
        null_regex: Some(regex::Regex::new("^NIL$").unwrap()),
        ..Default::default()
    };
    let format_custom_null = JsFormat::Csv(csv_format_custom_null.try_into().unwrap());
    check_serde(&format_custom_null);
}
