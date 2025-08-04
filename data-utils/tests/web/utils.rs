pub fn check_serde<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    use aqora_data_utils::wasm::serde::{from_value, to_value};
    let js_value = to_value(value).unwrap();
    // web_sys::console::log_1(&js_value);
    assert_eq!(value, &from_value::<T>(js_value).unwrap());
}
