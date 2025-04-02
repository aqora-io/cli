pub mod primitive;

pub use primitive::Primitive;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use primitive::PrimitiveType;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum CoerceError<P> {
    #[error("Cannot coerce {1:?} to {0:?}")]
    IncompatibleTypes(CoercedType, InferredType<P>),
}

impl<P> From<CoerceError<P>> for io::Error
where
    P: Send + Sync + fmt::Debug + 'static,
{
    fn from(err: CoerceError<P>) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

#[derive(Debug, Clone, Error)]
pub enum SchemaError {
    #[error("Cannot convert {0:?} into schema")]
    InvalidType(DataType),
}

impl From<SchemaError> for io::Error {
    fn from(err: SchemaError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

#[derive(Debug, Default, Clone)]
pub enum InferredType<P> {
    #[default]
    Null,
    Primitive(P),
    List(Vec<InferredType<P>>),
    Struct(HashMap<String, InferredType<P>>),
}

impl<'a, P> InferredType<P>
where
    P: PrimitiveType<'a>,
{
    fn coerce_self(self) -> Result<CoercedType, CoerceError<P>> {
        Ok(match self {
            InferredType::Null => CoercedType::Null,
            InferredType::Primitive(prim) => CoercedType::Primitive(prim.into(), false),
            InferredType::List(list) => {
                let mut iter = list.into_iter();
                let Some(first) = iter.next() else {
                    return Ok(CoercedType::List(Box::new(CoercedType::Null), false));
                };
                CoercedType::List(
                    Box::new(iter.try_fold(first.coerce_self()?, CoercedType::coerce)?),
                    false,
                )
            }
            InferredType::Struct(map) => CoercedType::Struct(
                map.into_iter()
                    .map(|(k, v)| Ok((k, v.coerce_self()?)))
                    .collect::<Result<_, _>>()?,
                false,
            ),
        })
    }
}

#[derive(Debug, Default, Clone)]
pub enum CoercedType {
    #[default]
    Null,
    Primitive(Primitive, bool),
    List(Box<CoercedType>, bool),
    Struct(HashMap<String, CoercedType>, bool),
}

impl CoercedType {
    fn coerce<'a, P>(self, other: InferredType<P>) -> Result<CoercedType, CoerceError<P>>
    where
        P: PrimitiveType<'a>,
    {
        Ok(match (self, other) {
            (CoercedType::Null, InferredType::Null) => CoercedType::Null,
            (CoercedType::Null, inferred) => {
                let mut coerced = inferred.coerce_self()?;
                coerced.set_nullable(true);
                coerced
            }
            (mut coerced, InferredType::Null) => {
                coerced.set_nullable(true);
                coerced
            }
            (CoercedType::Primitive(left, nullable), InferredType::Primitive(right)) => {
                CoercedType::Primitive(left.coerce(right.into()), nullable)
            }
            (CoercedType::List(coerced, nullable), InferredType::List(inferred)) => {
                CoercedType::List(
                    inferred
                        .into_iter()
                        .try_fold(coerced, |coerced, inferred| {
                            Ok(Box::new(coerced.coerce(inferred)?))
                        })?,
                    nullable,
                )
            }
            (CoercedType::List(list, nullable), inferred) => {
                CoercedType::List(Box::new(list.coerce(inferred)?), nullable)
            }
            (coerced, InferredType::List(list)) => CoercedType::List(
                Box::new(
                    list.into_iter()
                        .try_fold(coerced, |coerced, inferred| coerced.coerce(inferred))?,
                ),
                false,
            ),
            (CoercedType::Struct(mut coerced, nullable), InferredType::Struct(inferred)) => {
                let mut out = HashMap::new();
                for (k, v) in inferred {
                    if let Some(removed) = coerced.remove(&k) {
                        out.insert(k, removed.coerce(v)?);
                    } else {
                        out.insert(k, CoercedType::Null.coerce(v)?);
                    }
                }
                for (k, v) in coerced {
                    out.insert(k, v.coerce(InferredType::Null)?);
                }
                CoercedType::Struct(out, nullable)
            }
            (coerced @ CoercedType::Struct(..), inferred @ InferredType::Primitive(_))
            | (coerced @ CoercedType::Primitive(..), inferred @ InferredType::Struct(_)) => {
                return Err(CoerceError::IncompatibleTypes(coerced, inferred))
            }
        })
    }
}

impl CoercedType {
    pub fn set_nullable(&mut self, nullable: bool) {
        match self {
            CoercedType::Null => {}
            CoercedType::Primitive(_, n) | CoercedType::List(_, n) | CoercedType::Struct(_, n) => {
                *n = nullable
            }
        }
    }
    pub fn nullable(&self) -> bool {
        match self {
            CoercedType::Null => true,
            CoercedType::Primitive(_, nullable)
            | CoercedType::List(_, nullable)
            | CoercedType::Struct(_, nullable) => *nullable,
        }
    }
    pub fn datatype(self) -> DataType {
        match self {
            CoercedType::Null => DataType::Null,
            CoercedType::Primitive(prim, _) => prim.into(),
            CoercedType::List(ty, _) => {
                let nullable = ty.nullable();
                DataType::List(Arc::new(Field::new_list_field(ty.datatype(), nullable)))
            }
            CoercedType::Struct(map, _) => DataType::Struct(
                map.into_iter()
                    .map(|(name, ty)| ty.into_field(name))
                    .collect(),
            ),
        }
    }
    pub fn into_field(self, name: impl Into<String>) -> Field {
        let nullable = self.nullable();
        Field::new(name, self.datatype(), nullable)
    }
    pub fn into_schema(self) -> Result<Schema, SchemaError> {
        match self.datatype() {
            DataType::Struct(fields) => Ok(Schema::new(fields)),
            ty => Err(SchemaError::InvalidType(ty)),
        }
    }
}

#[derive(Default, Debug)]
pub enum CollectedType<P> {
    #[default]
    None,
    Ok(CoercedType),
    Err(CoerceError<P>),
}

impl<P> CollectedType<P> {
    pub fn into_result(self) -> Result<Option<CoercedType>, CoerceError<P>> {
        match self {
            CollectedType::None => Ok(None),
            CollectedType::Ok(coerced) => Ok(Some(coerced)),
            CollectedType::Err(err) => Err(err),
        }
    }
}

impl<P> CollectedType<P>
where
    P: Send + Sync + fmt::Debug + 'static,
{
    pub fn into_io_result(self) -> io::Result<CoercedType> {
        match self.into_result() {
            Ok(Some(coerced)) => Ok(coerced),
            Ok(None) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "No data collected",
            )),
            Err(err) => Err(err.into()),
        }
    }
}

impl<'a, T, P> Extend<T> for CollectedType<P>
where
    T: Into<InferredType<P>>,
    P: PrimitiveType<'a>,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        match self {
            CollectedType::None => *self = iter.into_iter().collect(),
            CollectedType::Ok(coerced) => {
                let value = std::mem::take(coerced);
                match iter
                    .into_iter()
                    .map(|i| i.into())
                    .try_fold(value, CoercedType::coerce)
                {
                    Ok(coerced) => *self = CollectedType::Ok(coerced),
                    Err(err) => *self = CollectedType::Err(err),
                }
            }
            CollectedType::Err(_) => {}
        };
    }
}

impl<'a, T, P> FromIterator<T> for CollectedType<P>
where
    T: Into<InferredType<P>>,
    P: PrimitiveType<'a>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut iter = iter.into_iter();
        if let Some(first) = iter.next() {
            let first = match first.into().coerce_self() {
                Ok(first) => first,
                Err(err) => return CollectedType::Err(err),
            };
            match iter.map(|i| i.into()).try_fold(first, CoercedType::coerce) {
                Ok(coerced) => CollectedType::Ok(coerced),
                Err(err) => CollectedType::Err(err),
            }
        } else {
            CollectedType::None
        }
    }
}

struct InferredTypeVisitor<P>(std::marker::PhantomData<P>);

impl<P> Default for InferredTypeVisitor<P> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<'de, P> Visitor<'de> for InferredTypeVisitor<P>
where
    P: PrimitiveType<'de>,
{
    type Value = InferredType<P>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid typed value")
    }

    #[inline]
    fn visit_bool<E>(self, value: bool) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_i8<E>(self, value: i8) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_i16<E>(self, value: i16) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_i32<E>(self, value: i32) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_i64<E>(self, value: i64) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_u8<E>(self, value: u8) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_u16<E>(self, value: u16) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_u32<E>(self, value: u32) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_u64<E>(self, value: u64) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_f32<E>(self, value: f32) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_f64<E>(self, value: f64) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.to_owned().into()))
    }

    #[inline]
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_bytes<E>(self, value: &[u8]) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.to_owned().into()))
    }

    #[inline]
    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<InferredType<P>, E> {
        Ok(InferredType::Primitive(value.into()))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<InferredType<P>, E> {
        Ok(InferredType::Null)
    }

    #[inline]
    fn visit_some<D>(self, deserializer: D) -> Result<InferredType<P>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<InferredType<P>, E> {
        Ok(InferredType::Null)
    }

    #[inline]
    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<InferredType<P>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    #[inline]
    fn visit_seq<V>(self, mut visitor: V) -> Result<InferredType<P>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut vec = Vec::new();
        while let Some(elem) = visitor.next_element()? {
            vec.push(elem);
        }
        Ok(InferredType::List(vec))
    }

    #[inline]
    fn visit_map<V>(self, mut visitor: V) -> Result<InferredType<P>, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut map = HashMap::new();
        while let Some((key, ty)) = visitor.next_entry()? {
            map.insert(key, ty);
        }
        Ok(InferredType::Struct(map))
    }
}

impl<'de, P> Deserialize<'de> for InferredType<P>
where
    P: PrimitiveType<'de>,
{
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<InferredType<P>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(InferredTypeVisitor::<P>::default())
    }
}

#[derive(Debug, Clone)]
pub struct InferredStruct<P>(InferredType<P>);

impl<P> From<InferredStruct<P>> for InferredType<P> {
    fn from(value: InferredStruct<P>) -> InferredType<P> {
        value.0
    }
}

impl<'de, P> Deserialize<'de> for InferredStruct<P>
where
    P: PrimitiveType<'de>,
{
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<InferredStruct<P>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(InferredStruct(
            deserializer.deserialize_map(InferredTypeVisitor::<P>::default())?,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct InferredList<P>(InferredType<P>);

impl<P> From<InferredList<P>> for InferredType<P> {
    fn from(value: InferredList<P>) -> InferredType<P> {
        value.0
    }
}

impl<'de, P> Deserialize<'de> for InferredList<P>
where
    P: PrimitiveType<'de>,
{
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<InferredList<P>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(InferredList(
            deserializer.deserialize_seq(InferredTypeVisitor::<P>::default())?,
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow::datatypes::Schema;
    use futures::stream::TryStreamExt;
    use futures::StreamExt;
    use std::collections::HashMap;
    use tokio::{fs, io};

    use crate::csv::Format;
    use crate::json::JsonlReadStream;
    use crate::schema::primitive::OwnedDataPrimitive;

    // async fn gather_json_examples() -> HashMap<String, (Vec<serde_json::Value>, Schema)> {
    //     let mut dir = fs::read_dir("./tests/data/json").await.unwrap();
    //     let mut out = HashMap::new();
    //     while let Some(entry) = dir.next_entry().await.unwrap() {
    //         let name = entry
    //             .path()
    //             .file_stem()
    //             .unwrap()
    //             .to_str()
    //             .unwrap()
    //             .to_string();
    //         let values = JsonlReadStream::new_default(fs::File::open(entry.path()).await.unwrap())
    //             .try_collect::<Vec<_>>()
    //             .await
    //             .unwrap();
    //         let schema = JsonlReadStream::<_, InferredStruct<Primitive>>::new_default(
    //             fs::File::open(entry.path()).await.unwrap(),
    //         )
    //         .try_collect::<CollectedType<_>>()
    //         .await
    //         .unwrap()
    //         .into_result()
    //         .unwrap()
    //         .unwrap()
    //         .into_schema()
    //         .unwrap();
    //         out.insert(name, (values, schema));
    //     }
    //     out
    // }

    async fn gather_csv_examples() -> HashMap<String, (Vec<serde_json::Value>, Schema)> {
        let mut dir = fs::read_dir("./tests/data/csv").await.unwrap();
        let mut out = HashMap::new();
        while let Some(entry) = dir.next_entry().await.unwrap() {
            let name = entry
                .path()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            if name != "example" {
                continue;
            }
            let schema = csv_async::AsyncReaderBuilder::from(Format {
                has_headers: true,
                ..Default::default()
            })
            .create_deserializer(fs::File::open(entry.path()).await.unwrap())
            .deserialize::<InferredStruct<OwnedDataPrimitive>>()
            .try_collect::<CollectedType<_>>()
            .await
            .unwrap()
            .into_result()
            .unwrap()
            .unwrap()
            .into_schema()
            .unwrap();
        }
        out
    }

    // #[tokio::test]
    // async fn test_create_schemas_from_json() {
    //     gather_json_examples().await;
    // }

    #[tokio::test]
    async fn test_create_schemas_from_csv() {
        gather_csv_examples().await;
    }
}
