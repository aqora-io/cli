use serde::{de, ser};

struct AsciiCharVisitor;

fn ascii_char_error<T, E>(value: T) -> E
where
    T: std::fmt::Display,
    E: de::Error,
{
    E::custom(format!("'{value}' is not an ASCII character"))
}

impl de::Visitor<'_> for AsciiCharVisitor {
    type Value = u8;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a single ASCII character")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value <= 0x7F {
            Ok(value as u8)
        } else {
            Err(ascii_char_error(value))
        }
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value >= 0 {
            self.visit_u64(value as u64)
        } else {
            Err(ascii_char_error(value))
        }
    }

    fn visit_char<E>(self, value: char) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_u64(value as u64)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut chars = value.chars();
        let Some(char) = chars.next() else {
            return Err(ascii_char_error(value));
        };
        if chars.next().is_none() {
            self.visit_char(char)
        } else {
            Err(ascii_char_error(value))
        }
    }
}

struct AsciiCharVisitorOpt;

impl<'de> de::Visitor<'de> for AsciiCharVisitorOpt {
    type Value = Option<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an optional single ASCII character")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AsciiCharVisitor.visit_u64(value).map(Some)
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AsciiCharVisitor.visit_i64(value).map(Some)
    }

    fn visit_char<E>(self, value: char) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AsciiCharVisitor.visit_char(value).map(Some)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AsciiCharVisitor.visit_str(value).map(Some)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_char(self)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }
}

pub mod ascii_char {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u8, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_char(AsciiCharVisitor)
    }

    pub fn serialize<S>(value: &u8, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        ser::Serialize::serialize(&char::from(*value), serializer)
    }
}

pub mod ascii_char_opt {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_char(AsciiCharVisitorOpt)
    }

    pub fn serialize<S>(value: &Option<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match value {
            Some(value) => ser::Serialize::serialize(&char::from(*value), serializer),
            None => ser::Serialize::serialize(&Option::<char>::None, serializer),
        }
    }
}

pub mod regex_opt {
    use super::*;
    use regex::Regex;
    use serde::de::{Deserialize, Error};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Regex>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer).and_then(|s| {
            s.as_deref()
                .map(Regex::new)
                .transpose()
                .map_err(D::Error::custom)
        })
    }

    pub fn serialize<S>(value: &Option<Regex>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match value {
            Some(value) => ser::Serialize::serialize(value.as_str(), serializer),
            None => ser::Serialize::serialize(&Option::<String>::None, serializer),
        }
    }
}
