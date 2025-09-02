use std::{collections::HashMap, fmt, ops::Deref, sync::Arc};

use arrow::datatypes::{DataType, Field, FieldRef, Fields, IntervalUnit, TimeUnit, UnionMode};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SerdeArrowSchema, Strategy};

pub use serde_arrow::marrow;

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Fields,
    metadata: IndexMap<String, String>,
}

impl Schema {
    pub fn new(fields: Fields) -> Self {
        Self {
            fields,
            metadata: Default::default(),
        }
    }

    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    pub fn metadata(&self) -> &IndexMap<String, String> {
        &self.metadata
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        SerdeSchema::from(self) == SerdeSchema::from(other)
    }
}
impl Eq for Schema {}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(SerdeSchema::deserialize(deserializer)?.into())
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerdeSchema::from(self).serialize(serializer)
    }
}

impl TryFrom<Schema> for SerdeArrowSchema {
    type Error = serde_arrow::Error;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        SerdeArrowSchema::try_from(value.fields.deref())
    }
}

impl TryFrom<&Schema> for SerdeArrowSchema {
    type Error = serde_arrow::Error;

    fn try_from(value: &Schema) -> Result<Self, Self::Error> {
        SerdeArrowSchema::try_from(value.fields.deref())
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

impl From<arrow::datatypes::Schema> for Schema {
    fn from(value: arrow::datatypes::Schema) -> Self {
        Self {
            fields: value.fields,
            metadata: value.metadata.into_iter().collect(),
        }
    }
}

impl From<arrow::datatypes::SchemaRef> for Schema {
    fn from(value: arrow::datatypes::SchemaRef) -> Self {
        value.as_ref().clone().into()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "wasm", derive(ts_rs::TS), ts(rename = "Schema", export))]
pub struct SerdeSchema {
    fields: Vec<SerdeField>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<HashMap<String, String>>"))]
    metadata: IndexMap<String, String>,
}

impl From<&Schema> for SerdeSchema {
    fn from(value: &Schema) -> Self {
        Self {
            fields: value
                .fields
                .iter()
                .map(|field| SerdeField::from(field.as_ref()))
                .collect(),
            metadata: value.metadata.clone(),
        }
    }
}

impl From<Schema> for SerdeSchema {
    fn from(value: Schema) -> Self {
        Self {
            fields: value
                .fields
                .iter()
                .map(|field| SerdeField::from(field.as_ref()))
                .collect(),
            metadata: value.metadata,
        }
    }
}

impl From<SerdeSchema> for Schema {
    fn from(value: SerdeSchema) -> Self {
        Self {
            fields: value.fields.into_iter().map(Field::from).collect(),
            metadata: value.metadata.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaField", export)
)]
pub struct SerdeField {
    name: String,
    #[serde(flatten)]
    data_type: SerdeDataType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    strategy: Option<SerdeStrategy>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
    nullable: bool,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    #[cfg_attr(feature = "wasm", ts(optional, as = "Option<HashMap<String, String>>"))]
    metadata: IndexMap<String, String>,
}

const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

impl From<&Field> for SerdeField {
    fn from(value: &Field) -> SerdeField {
        let mut metadata: IndexMap<String, String> = value
            .metadata()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let strategy = if let Some(value) = metadata.get(STRATEGY_KEY) {
            if let Ok(strategy) =
                SerdeStrategy::deserialize(serde::de::value::StringDeserializer::<
                    serde::de::value::Error,
                >::new(value.into()))
            {
                metadata.swap_remove(STRATEGY_KEY);
                Some(strategy)
            } else {
                None
            }
        } else {
            None
        };
        SerdeField {
            name: value.name().to_string(),
            data_type: SerdeDataType::from(value.data_type()),
            nullable: value.is_nullable(),
            strategy,
            metadata,
        }
    }
}

impl From<SerdeField> for Field {
    fn from(value: SerdeField) -> Field {
        let mut metadata: HashMap<String, String> = value.metadata.into_iter().collect();
        if let Some(strategy) = value.strategy {
            metadata.insert(STRATEGY_KEY.to_string(), strategy.to_string());
        }
        Field::new(value.name, value.data_type.into(), value.nullable).with_metadata(metadata)
    }
}

impl From<SerdeField> for FieldRef {
    fn from(value: SerdeField) -> FieldRef {
        Arc::new(value.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaStrategy", export)
)]
pub enum SerdeStrategy {
    InconsistentTypes,
    TupleAsStruct,
    MapAsStruct,
    UnknownVariant,
}

pub struct StrategyNotFound;

impl TryFrom<Strategy> for SerdeStrategy {
    type Error = StrategyNotFound;
    fn try_from(strategy: Strategy) -> Result<Self, Self::Error> {
        Ok(match strategy {
            Strategy::InconsistentTypes => SerdeStrategy::InconsistentTypes,
            Strategy::TupleAsStruct => SerdeStrategy::TupleAsStruct,
            Strategy::MapAsStruct => SerdeStrategy::MapAsStruct,
            Strategy::UnknownVariant => SerdeStrategy::UnknownVariant,
            _ => return Err(StrategyNotFound),
        })
    }
}

impl From<SerdeStrategy> for Strategy {
    fn from(strategy: SerdeStrategy) -> Self {
        match strategy {
            SerdeStrategy::InconsistentTypes => Strategy::InconsistentTypes,
            SerdeStrategy::TupleAsStruct => Strategy::TupleAsStruct,
            SerdeStrategy::MapAsStruct => Strategy::MapAsStruct,
            SerdeStrategy::UnknownVariant => Strategy::UnknownVariant,
        }
    }
}

impl fmt::Display for SerdeStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.serialize(f)
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaTimeUnit", export)
)]
pub enum SerdeTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<TimeUnit> for SerdeTimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => SerdeTimeUnit::Second,
            TimeUnit::Millisecond => SerdeTimeUnit::Millisecond,
            TimeUnit::Microsecond => SerdeTimeUnit::Microsecond,
            TimeUnit::Nanosecond => SerdeTimeUnit::Nanosecond,
        }
    }
}

impl From<SerdeTimeUnit> for TimeUnit {
    fn from(unit: SerdeTimeUnit) -> Self {
        match unit {
            SerdeTimeUnit::Second => TimeUnit::Second,
            SerdeTimeUnit::Millisecond => TimeUnit::Millisecond,
            SerdeTimeUnit::Microsecond => TimeUnit::Microsecond,
            SerdeTimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaIntervalUnit", export)
)]
pub enum SerdeIntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

impl From<IntervalUnit> for SerdeIntervalUnit {
    fn from(unit: IntervalUnit) -> Self {
        match unit {
            IntervalUnit::YearMonth => SerdeIntervalUnit::YearMonth,
            IntervalUnit::DayTime => SerdeIntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => SerdeIntervalUnit::MonthDayNano,
        }
    }
}

impl From<SerdeIntervalUnit> for IntervalUnit {
    fn from(unit: SerdeIntervalUnit) -> Self {
        match unit {
            SerdeIntervalUnit::YearMonth => IntervalUnit::YearMonth,
            SerdeIntervalUnit::DayTime => IntervalUnit::DayTime,
            SerdeIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaUnionMode", export)
)]
pub enum SerdeUnionMode {
    Sparse,
    Dense,
}

impl From<UnionMode> for SerdeUnionMode {
    fn from(mode: UnionMode) -> Self {
        match mode {
            UnionMode::Sparse => SerdeUnionMode::Sparse,
            UnionMode::Dense => SerdeUnionMode::Dense,
        }
    }
}

impl From<SerdeUnionMode> for UnionMode {
    fn from(mode: SerdeUnionMode) -> Self {
        match mode {
            SerdeUnionMode::Sparse => UnionMode::Sparse,
            SerdeUnionMode::Dense => UnionMode::Dense,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaUnionField", export)
)]
pub struct SerdeUnionField {
    pub type_id: i8,
    pub field: SerdeField,
}

impl From<(i8, &Field)> for SerdeUnionField {
    fn from((type_id, field): (i8, &Field)) -> SerdeUnionField {
        SerdeUnionField {
            type_id,
            field: SerdeField::from(field),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "SchemaDataType", export)
)]
#[serde(tag = "data_type")]
pub enum SerdeDataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Timestamp {
        unit: SerdeTimeUnit,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "wasm", ts(optional))]
        tz: Option<String>,
    },
    Date32,
    Date64,
    Time32 {
        unit: SerdeTimeUnit,
    },
    Time64 {
        unit: SerdeTimeUnit,
    },
    Duration {
        unit: SerdeTimeUnit,
    },
    Interval {
        unit: SerdeIntervalUnit,
    },
    Binary,
    FixedSizeBinary {
        size: i32,
    },
    LargeBinary,
    BinaryView,
    Utf8,
    LargeUtf8,
    Utf8View,
    List {
        field: Box<SerdeField>,
    },
    ListView {
        field: Box<SerdeField>,
    },
    FixedSizeList {
        field: Box<SerdeField>,
        size: i32,
    },
    LargeList {
        field: Box<SerdeField>,
    },
    LargeListView {
        field: Box<SerdeField>,
    },
    Struct {
        fields: Vec<SerdeField>,
    },
    Union {
        fields: Vec<SerdeUnionField>,
        mode: SerdeUnionMode,
    },
    Dictionary {
        key: Box<SerdeDataType>,
        value: Box<SerdeDataType>,
    },
    Decimal32 {
        precision: u8,
        scale: i8,
    },
    Decimal64 {
        precision: u8,
        scale: i8,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    Map {
        field: Box<SerdeField>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        #[cfg_attr(feature = "wasm", ts(optional, as = "Option<bool>"))]
        sorted: bool,
    },
    RunEndEncoded {
        run_ends: Box<SerdeField>,
        values: Box<SerdeField>,
    },
}

impl From<&DataType> for SerdeDataType {
    fn from(value: &DataType) -> SerdeDataType {
        match value {
            DataType::Null => SerdeDataType::Null,
            DataType::Boolean => SerdeDataType::Boolean,
            DataType::Int8 => SerdeDataType::Int8,
            DataType::Int16 => SerdeDataType::Int16,
            DataType::Int32 => SerdeDataType::Int32,
            DataType::Int64 => SerdeDataType::Int64,
            DataType::UInt8 => SerdeDataType::UInt8,
            DataType::UInt16 => SerdeDataType::UInt16,
            DataType::UInt32 => SerdeDataType::UInt32,
            DataType::UInt64 => SerdeDataType::UInt64,
            DataType::Float16 => SerdeDataType::Float16,
            DataType::Float32 => SerdeDataType::Float32,
            DataType::Float64 => SerdeDataType::Float64,
            DataType::Timestamp(unit, tz) => SerdeDataType::Timestamp {
                unit: (*unit).into(),
                tz: tz.as_ref().map(|tz| tz.to_string()),
            },
            DataType::Date32 => SerdeDataType::Date32,
            DataType::Date64 => SerdeDataType::Date64,
            DataType::Time32(unit) => SerdeDataType::Time32 {
                unit: (*unit).into(),
            },
            DataType::Time64(unit) => SerdeDataType::Time64 {
                unit: (*unit).into(),
            },
            DataType::Duration(unit) => SerdeDataType::Duration {
                unit: (*unit).into(),
            },
            DataType::Interval(unit) => SerdeDataType::Interval {
                unit: (*unit).into(),
            },
            DataType::Binary => SerdeDataType::Binary,
            DataType::FixedSizeBinary(size) => SerdeDataType::FixedSizeBinary { size: *size },
            DataType::LargeBinary => SerdeDataType::LargeBinary,
            DataType::BinaryView => SerdeDataType::BinaryView,
            DataType::Utf8 => SerdeDataType::Utf8,
            DataType::LargeUtf8 => SerdeDataType::LargeUtf8,
            DataType::Utf8View => SerdeDataType::Utf8View,
            DataType::List(field) => SerdeDataType::List {
                field: Box::new(field.as_ref().into()),
            },
            DataType::ListView(field) => SerdeDataType::ListView {
                field: Box::new(field.as_ref().into()),
            },
            DataType::FixedSizeList(field, size) => SerdeDataType::FixedSizeList {
                field: Box::new(field.as_ref().into()),
                size: *size,
            },
            DataType::LargeList(field) => SerdeDataType::LargeList {
                field: Box::new(field.as_ref().into()),
            },
            DataType::LargeListView(field) => SerdeDataType::LargeListView {
                field: Box::new(field.as_ref().into()),
            },
            DataType::Struct(fields) => SerdeDataType::Struct {
                fields: fields
                    .iter()
                    .map(|f| SerdeField::from(f.as_ref()))
                    .collect(),
            },
            DataType::Union(fields, mode) => SerdeDataType::Union {
                fields: fields
                    .iter()
                    .map(|(type_id, field)| SerdeUnionField::from((type_id, field.as_ref())))
                    .collect(),
                mode: (*mode).into(),
            },
            DataType::Dictionary(key, value) => SerdeDataType::Dictionary {
                key: Box::new(key.as_ref().into()),
                value: Box::new(value.as_ref().into()),
            },
            DataType::Decimal32(precision, scale) => SerdeDataType::Decimal32 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal64(precision, scale) => SerdeDataType::Decimal64 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal128(precision, scale) => SerdeDataType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal256(precision, scale) => SerdeDataType::Decimal256 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Map(field, sorted) => SerdeDataType::Map {
                field: Box::new(field.as_ref().into()),
                sorted: *sorted,
            },
            DataType::RunEndEncoded(run_ends, values) => SerdeDataType::RunEndEncoded {
                run_ends: Box::new(run_ends.as_ref().into()),
                values: Box::new(values.as_ref().into()),
            },
        }
    }
}

impl From<SerdeDataType> for DataType {
    fn from(value: SerdeDataType) -> DataType {
        match value {
            SerdeDataType::Null => DataType::Null,
            SerdeDataType::Boolean => DataType::Boolean,
            SerdeDataType::Int8 => DataType::Int8,
            SerdeDataType::Int16 => DataType::Int16,
            SerdeDataType::Int32 => DataType::Int32,
            SerdeDataType::Int64 => DataType::Int64,
            SerdeDataType::UInt8 => DataType::UInt8,
            SerdeDataType::UInt16 => DataType::UInt16,
            SerdeDataType::UInt32 => DataType::UInt32,
            SerdeDataType::UInt64 => DataType::UInt64,
            SerdeDataType::Float16 => DataType::Float16,
            SerdeDataType::Float32 => DataType::Float32,
            SerdeDataType::Float64 => DataType::Float64,
            SerdeDataType::Timestamp { unit, tz } => {
                DataType::Timestamp(unit.into(), tz.map(|s| s.into()))
            }
            SerdeDataType::Date32 => DataType::Date32,
            SerdeDataType::Date64 => DataType::Date64,
            SerdeDataType::Time32 { unit } => DataType::Time32(unit.into()),
            SerdeDataType::Time64 { unit } => DataType::Time64(unit.into()),
            SerdeDataType::Duration { unit } => DataType::Duration(unit.into()),
            SerdeDataType::Interval { unit } => DataType::Interval(unit.into()),
            SerdeDataType::Binary => DataType::Binary,
            SerdeDataType::FixedSizeBinary { size } => DataType::FixedSizeBinary(size),
            SerdeDataType::LargeBinary => DataType::LargeBinary,
            SerdeDataType::BinaryView => DataType::BinaryView,
            SerdeDataType::Utf8 => DataType::Utf8,
            SerdeDataType::LargeUtf8 => DataType::LargeUtf8,
            SerdeDataType::Utf8View => DataType::Utf8View,
            SerdeDataType::List { field } => DataType::List((*field).into()),
            SerdeDataType::ListView { field } => DataType::ListView((*field).into()),
            SerdeDataType::FixedSizeList { field, size } => {
                DataType::FixedSizeList((*field).into(), size)
            }
            SerdeDataType::LargeList { field } => DataType::LargeList((*field).into()),
            SerdeDataType::LargeListView { field } => DataType::LargeListView((*field).into()),
            SerdeDataType::Struct { fields } => {
                DataType::Struct(fields.into_iter().map(FieldRef::from).collect())
            }
            SerdeDataType::Union { fields, mode } => DataType::Union(
                fields
                    .into_iter()
                    .map(|SerdeUnionField { type_id, field }| (type_id, FieldRef::from(field)))
                    .collect(),
                mode.into(),
            ),
            SerdeDataType::Dictionary { key, value } => {
                DataType::Dictionary(Box::new((*key).into()), Box::new((*value).into()))
            }
            SerdeDataType::Decimal32 { precision, scale } => DataType::Decimal32(precision, scale),
            SerdeDataType::Decimal64 { precision, scale } => DataType::Decimal64(precision, scale),
            SerdeDataType::Decimal128 { precision, scale } => {
                DataType::Decimal128(precision, scale)
            }
            SerdeDataType::Decimal256 { precision, scale } => {
                DataType::Decimal256(precision, scale)
            }
            SerdeDataType::Map { field, sorted } => DataType::Map((*field).into(), sorted),
            SerdeDataType::RunEndEncoded { run_ends, values } => {
                DataType::RunEndEncoded((*run_ends).into(), (*values).into())
            }
        }
    }
}
