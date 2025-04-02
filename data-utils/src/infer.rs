use arrow::datatypes::FieldRef;
use indexmap::IndexMap;
use serde::{de::Error as _, ser::Error as _, Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;

pub use serde_arrow::{
    marrow,
    schema::{Overwrites, SerdeArrowSchema, TracingOptions as Options},
};

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<FieldRef>,
    metadata: IndexMap<String, String>,
}

impl Schema {
    pub fn new(fields: Vec<FieldRef>) -> Self {
        Self {
            fields,
            metadata: Default::default(),
        }
    }

    pub fn fields(&self) -> &[FieldRef] {
        &self.fields
    }

    pub fn metadata(&self) -> &IndexMap<String, String> {
        &self.metadata
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let schema = serde_json::Map::deserialize(deserializer)?;
        let metadata = schema
            .get("metadata")
            .map(|m| match m {
                serde_json::Value::Object(map) => {
                    let mut out = IndexMap::new();
                    for (key, value) in map {
                        match value {
                            serde_json::Value::String(value) => {
                                out.insert(key.to_string(), value.to_string());
                            }
                            serde_json::Value::Null => {}
                            _ => {
                                return Err(D::Error::custom(
                                    "Expected metadata values to be a string",
                                ))
                            }
                        }
                    }
                    Ok(out)
                }
                _ => Err(D::Error::custom("Expected metadata to be a Map")),
            })
            .transpose()
            .map_err(D::Error::custom)?
            .unwrap_or_default();
        let serde_schema: SerdeArrowSchema = serde_json::to_string(&schema)
            .and_then(|s| serde_json::from_str(&s))
            .map_err(D::Error::custom)?;
        Ok(Schema {
            fields: serde_schema.try_into().map_err(D::Error::custom)?,
            metadata,
        })
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serde_schema =
            SerdeArrowSchema::try_from(self.fields.as_slice()).map_err(S::Error::custom)?;
        let mut map: serde_json::Map<String, serde_json::Value> =
            serde_json::to_string(&serde_schema)
                .and_then(|s| serde_json::from_str(&s))
                .map_err(S::Error::custom)?;
        map.insert(
            "metadata".to_string(),
            serde_json::Value::Object(
                self.metadata
                    .iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.into())))
                    .collect::<serde_json::Map<String, serde_json::Value>>(),
            ),
        );
        map.serialize(serializer)
    }
}

impl TryFrom<Schema> for SerdeArrowSchema {
    type Error = serde_arrow::Error;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        SerdeArrowSchema::try_from(value.fields.as_slice())
    }
}

impl From<Schema> for arrow::datatypes::Schema {
    fn from(value: Schema) -> Self {
        arrow::datatypes::Schema::new_with_metadata(
            value.fields,
            value.metadata.into_iter().collect(),
        )
    }
}

pub fn from_samples<T>(samples: &[T], options: Options) -> Result<Schema, serde_arrow::Error>
where
    T: Serialize,
{
    Ok(Schema::new(<Vec<FieldRef>>::from_samples(
        samples, options,
    )?))
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
    SampleDebug { schema, error }
}
