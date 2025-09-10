use std::collections::HashMap;

use arrow::datatypes::FieldRef;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};

use crate::schema::{Schema, SerdeField};

mod serde_overwrites {
    use super::*;

    pub fn serialize<S: serde::Serializer>(
        val: &HashMap<String, FieldRef>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        let map: HashMap<String, SerdeField> = val
            .iter()
            .map(|(k, v)| (k.to_string(), SerdeField::from(v.as_ref())))
            .collect();
        map.serialize(ser)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        de: D,
    ) -> Result<HashMap<String, FieldRef>, D::Error> {
        use serde::Deserialize;
        let val = HashMap::<String, SerdeField>::deserialize(de)?;
        Ok(val
            .into_iter()
            .map(|(k, v)| (k, FieldRef::from(v)))
            .collect())
    }
}

const fn default_from_type_budget() -> usize {
    100
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(export, rename = "InferOptions")
)]
pub struct Options {
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub forbid_null_fields: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub no_map_as_struct: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub sequence_as_small_list: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub string_as_small_utf8: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub bytes_as_small_binary: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub string_dictionary_encoding: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub no_coerce_numbers: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub forbid_to_string: bool,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub no_guess_dates: bool,
    #[serde(default = "default_from_type_budget")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<usize>"))]
    pub from_type_budget: usize,
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    pub no_unit_enum_as_string: bool,
    #[serde(default, with = "serde_overwrites")]
    #[cfg_attr(
        feature = "wasm",
        ts(optional, as = "Option<HashMap<String, SerdeField>>")
    )]
    pub overwrites: HashMap<String, FieldRef>,
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn overwrite(&mut self, path: impl Into<String>, field: impl Into<FieldRef>) {
        self.overwrites.insert(path.into(), field.into());
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            forbid_null_fields: false,
            no_map_as_struct: false,
            sequence_as_small_list: false,
            string_as_small_utf8: false,
            bytes_as_small_binary: false,
            string_dictionary_encoding: false,
            no_coerce_numbers: false,
            forbid_to_string: false,
            no_guess_dates: false,
            from_type_budget: default_from_type_budget(),
            no_unit_enum_as_string: false,
            overwrites: HashMap::new(),
        }
    }
}

fn field_to_serde_arrow_overwrite(
    field: FieldRef,
) -> Result<serde_json::Value, serde_arrow::Error> {
    let schema = serde_arrow::schema::SerdeArrowSchema::try_from([field].as_slice())?;
    if let serde_json::Value::Object(mut map) =
        serde_json::to_value(schema).map_err(|err| serde_arrow::Error::custom(err.to_string()))?
    {
        if let Some(serde_json::Value::Array(mut arr)) = map.remove("fields") {
            if let Some(field) = arr.pop() {
                return Ok(field);
            }
        }
    }
    Err(serde_arrow::Error::custom(
        "Invalid JSON returned for SerdeArrowSchema".into(),
    ))
}

impl TryFrom<Options> for TracingOptions {
    type Error = serde_arrow::Error;
    fn try_from(value: Options) -> Result<Self, Self::Error> {
        let mut options = TracingOptions::default()
            .allow_null_fields(!value.forbid_null_fields)
            .coerce_numbers(!value.no_coerce_numbers)
            .allow_to_string(!value.forbid_to_string)
            .map_as_struct(!value.no_map_as_struct)
            .sequence_as_large_list(!value.sequence_as_small_list)
            .strings_as_large_utf8(!value.string_as_small_utf8)
            .bytes_as_large_binary(!value.bytes_as_small_binary)
            .enums_without_data_as_strings(value.no_unit_enum_as_string)
            .string_dictionary_encoding(value.string_dictionary_encoding)
            .guess_dates(!value.no_guess_dates)
            .from_type_budget(value.from_type_budget);
        for (path, field) in value.overwrites {
            let field = field_to_serde_arrow_overwrite(field)?;
            options = options.overwrite(path, field)?;
        }
        Ok(options)
    }
}

pub async fn take_samples<S>(
    stream: &mut S,
    sample_size: Option<usize>,
) -> Result<Vec<S::Ok>, S::Error>
where
    S: TryStream + Unpin,
{
    let mut samples = if let Some(sample_size) = sample_size {
        Vec::with_capacity(sample_size)
    } else {
        Vec::new()
    };
    while let Some(value) = stream.try_next().await? {
        samples.push(value);
        if sample_size.is_some_and(|s| samples.len() >= s) {
            break;
        }
    }
    Ok(samples)
}

pub fn from_samples<T>(samples: &[T], options: Options) -> Result<Schema, serde_arrow::Error>
where
    T: Serialize,
{
    Ok(Schema::new(
        <Vec<FieldRef>>::from_samples(samples, options.try_into()?)?
            .into_iter()
            .collect(),
    ))
}

#[derive(Debug)]
pub struct SampleDebug {
    pub schema: Option<Schema>,
    pub error: Option<(usize, serde_arrow::Error)>,
}

impl SampleDebug {
    pub fn is_ok(&self) -> bool {
        self.error.is_none() && self.schema.is_some()
    }

    pub fn select_err<'a, T>(&self, samples: &'a [T]) -> Option<&'a T> {
        self.error
            .as_ref()
            .and_then(|(index, _)| samples.get(*index))
    }
}

pub fn debug_samples<T>(samples: &[T], options: Options) -> SampleDebug
where
    T: Serialize,
{
    let mut left = 0;
    let mut right = samples.len();
    let mut schema = None;
    let mut error = None;

    while left < right {
        let mid = left + (right - left) / 2;
        match from_samples(&samples[..mid], options.clone()) {
            Ok(s) => {
                schema = Some(s);
                left = mid + 1;
            }
            Err(e) => {
                if mid != 0 {
                    error = Some((mid - 1, e));
                }
                right = mid;
            }
        }
    }
    if left == samples.len() {
        match from_samples(samples, options.clone()) {
            Ok(s) => {
                schema = Some(s);
            }
            Err(e) => {
                error = Some((samples.len() - 1, e));
            }
        }
    }
    SampleDebug { schema, error }
}
