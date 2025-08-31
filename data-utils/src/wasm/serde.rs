use js_sys::{Array, JsString, Object};
use wasm_bindgen::prelude::*;

pub use serde_wasm_bindgen::Error;

pub const DEFAULT_SERIALIZER: serde_wasm_bindgen::Serializer =
    serde_wasm_bindgen::Serializer::new()
        .serialize_maps_as_objects(true)
        .serialize_large_number_types_as_bigints(true);

pub fn to_value<T: serde::Serialize + ?Sized>(value: &T) -> Result<JsValue, Error> {
    value.serialize(&DEFAULT_SERIALIZER)
}

fn keep_defined_keys(value: JsValue) -> Result<JsValue, JsValue> {
    Ok(if let Some(array) = value.dyn_ref::<js_sys::Array>() {
        let out = js_sys::Array::new();
        for value in array.iter() {
            out.push(&keep_defined_keys(value)?);
        }
        out.into()
    } else if let Some(object) = value.dyn_ref::<js_sys::Object>() {
        let constructor = object.constructor();
        if constructor.is_undefined() || constructor == js_sys::Object::new().constructor() {
            let out = js_sys::Object::new();
            for entry in Object::entries(object) {
                let kv = entry.unchecked_ref::<Array>();
                let key = kv.get(0);
                let value = kv.get(1);
                if value.is_undefined() {
                    continue;
                }
                js_sys::Reflect::set(&out, &key, &keep_defined_keys(value)?)?;
            }
            out.into()
        } else {
            value
        }
    } else {
        value
    })
}

pub fn from_value<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, Error> {
    serde_wasm_bindgen::from_value(keep_defined_keys(value)?)
}

// A workaround implement the Deserialize trait because of problems with internally tagged
// enums https://github.com/RReverser/serde-wasm-bindgen/issues/73
pub trait DeserializeTagged<'de>: Sized {
    const TAG: &'static str;
    type Tag: serde::de::DeserializeOwned;

    fn deserialize_tagged<D>(tag: Self::Tag, deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>;

    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Deserialize as _, Error as _};
        let value: Object = serde_wasm_bindgen::preserve::deserialize(deserializer)?;
        let entries = Object::entries(&value);
        let mut tag = None;
        for (index, entry) in entries.iter().enumerate() {
            let kv = entry.unchecked_ref::<Array>();
            if let Ok(key_str) = kv.get(0).dyn_into::<JsString>() {
                if String::from(key_str).as_str() == Self::TAG {
                    tag = Some(
                        Self::Tag::deserialize(serde_wasm_bindgen::Deserializer::from(kv.get(1)))
                            .map_err(D::Error::custom)?,
                    );
                    let _ = entries.splice(index as u32, 1, &Array::new());
                    break;
                }
            }
        }
        let tag = tag.ok_or_else(|| {
            D::Error::custom(format!(r#""{}" key not found in object"#, Self::TAG))
        })?;
        let object = Object::from_entries(&entries).map_err(|err| {
            D::Error::custom(format!(
                r#"Could not create an object from entries: {err:?}"#,
            ))
        })?;
        Self::deserialize_tagged(
            tag,
            serde_wasm_bindgen::Deserializer::from(JsValue::from(object)),
        )
        .map_err(D::Error::custom)
    }
}

pub mod preserve {
    pub use serde_wasm_bindgen::preserve::*;

    pub mod option {
        pub fn serialize<S: serde::Serializer, T: wasm_bindgen::JsCast>(
            val: &Option<T>,
            ser: S,
        ) -> Result<S::Ok, S::Error> {
            match val {
                None => ser.serialize_none(),
                Some(v) => super::serialize(v, ser),
            }
        }

        struct OptionVisitor<T>(std::marker::PhantomData<T>);

        impl<T> Default for OptionVisitor<T> {
            fn default() -> Self {
                Self(std::marker::PhantomData)
            }
        }

        impl<'de, T> serde::de::Visitor<'de> for OptionVisitor<T>
        where
            T: wasm_bindgen::JsCast,
        {
            type Value = Option<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JS value or null or undefined")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(None)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D>(self, de: D) -> Result<Self::Value, D::Error>
            where
                D: serde::de::Deserializer<'de>,
            {
                Ok(Some(super::deserialize(de)?))
            }
        }

        pub fn deserialize<'de, D: serde::Deserializer<'de>, T: wasm_bindgen::JsCast>(
            de: D,
        ) -> Result<Option<T>, D::Error> {
            de.deserialize_option(OptionVisitor::<T>::default())
        }
    }
}
