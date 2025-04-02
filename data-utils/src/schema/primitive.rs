use std::fmt;

use arrow::datatypes::DataType;
use bytes::Bytes;
use std::borrow::Cow;

pub trait PrimitiveType<'a>:
    From<bool>
    + From<u8>
    + From<u16>
    + From<u32>
    + From<u64>
    + From<i8>
    + From<i16>
    + From<i32>
    + From<i64>
    + From<f32>
    + From<f64>
    + From<f64>
    + From<&'a str>
    + From<&'a [u8]>
    + From<String>
    + From<Vec<u8>>
    + Into<Primitive>
    + fmt::Debug
    + Clone
{
}

impl<'a, T> PrimitiveType<'a> for T where
    T: From<bool>
        + From<u8>
        + From<u16>
        + From<u32>
        + From<u64>
        + From<i8>
        + From<i16>
        + From<i32>
        + From<i64>
        + From<f32>
        + From<f64>
        + From<f64>
        + From<&'a str>
        + From<&'a [u8]>
        + From<String>
        + From<Vec<u8>>
        + Into<Primitive>
        + fmt::Debug
        + Clone
{
}

#[derive(Debug, Copy, Clone)]
pub enum Primitive {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
}

impl Primitive {
    pub fn coerce(self, other: Self) -> Self {
        use Primitive::*;
        match (self, other) {
            (Binary, _) | (_, Binary) => Binary,
            (Utf8, _) | (_, Utf8) => Utf8,
            (Float64, _) | (_, Float64) => Float64,
            (Float32, UInt64)
            | (UInt64, Float32)
            | (Float32, UInt32)
            | (UInt32, Float32)
            | (Float32, Int64)
            | (Int64, Float32) => Float64,
            (Float32, _) | (_, Float32) => Float32,
            (Int64, _) | (_, Int64) => Int64,
            (Int32, UInt64) | (UInt64, Int32) => Int64,
            (Int32, _) | (_, Int32) => Int32,
            (Int16, UInt64) | (UInt64, Int16) | (Int16, UInt32) | (UInt32, Int16) => Int64,
            (Int16, UInt16) | (UInt16, Int16) => Int32,
            (Int16, _) | (_, Int16) => Int16,
            (Int8, UInt64) | (UInt64, Int8) | (Int8, UInt32) | (UInt32, Int8) => Int64,
            (Int8, UInt16) | (UInt16, Int8) => Int32,
            (Int8, UInt8) | (UInt8, Int8) => Int16,
            (Int8, _) | (_, Int8) => UInt8,
            (UInt64, _) | (_, UInt64) => UInt64,
            (UInt32, _) | (_, UInt32) => UInt32,
            (UInt16, _) | (_, UInt16) => UInt16,
            (UInt8, _) | (_, UInt8) => UInt8,
            (Boolean, Boolean) => Boolean,
        }
    }
}

impl From<Primitive> for DataType {
    fn from(value: Primitive) -> Self {
        match value {
            Primitive::Boolean => DataType::Boolean,
            Primitive::Int8 => DataType::Int8,
            Primitive::Int16 => DataType::Int16,
            Primitive::Int32 => DataType::Int32,
            Primitive::Int64 => DataType::Int64,
            Primitive::UInt8 => DataType::UInt8,
            Primitive::UInt16 => DataType::UInt16,
            Primitive::UInt32 => DataType::UInt32,
            Primitive::UInt64 => DataType::UInt64,
            Primitive::Float32 => DataType::Float32,
            Primitive::Float64 => DataType::Float64,
            Primitive::Utf8 => DataType::Utf8,
            Primitive::Binary => DataType::Binary,
        }
    }
}

impl From<bool> for Primitive {
    fn from(_: bool) -> Primitive {
        Primitive::Boolean
    }
}

impl From<u8> for Primitive {
    fn from(_: u8) -> Primitive {
        Primitive::UInt8
    }
}

impl From<u16> for Primitive {
    fn from(_: u16) -> Primitive {
        Primitive::UInt16
    }
}

impl From<u32> for Primitive {
    fn from(_: u32) -> Primitive {
        Primitive::UInt32
    }
}

impl From<u64> for Primitive {
    fn from(_: u64) -> Primitive {
        Primitive::UInt64
    }
}

impl From<i8> for Primitive {
    fn from(_: i8) -> Primitive {
        Primitive::Int8
    }
}

impl From<i16> for Primitive {
    fn from(_: i16) -> Primitive {
        Primitive::Int16
    }
}

impl From<i32> for Primitive {
    fn from(_: i32) -> Primitive {
        Primitive::Int32
    }
}

impl From<i64> for Primitive {
    fn from(_: i64) -> Primitive {
        Primitive::Int64
    }
}

impl From<f32> for Primitive {
    fn from(_: f32) -> Primitive {
        Primitive::Float32
    }
}

impl From<f64> for Primitive {
    fn from(_: f64) -> Primitive {
        Primitive::Float64
    }
}

impl<'a> From<&'a str> for Primitive {
    fn from(_: &'a str) -> Primitive {
        Primitive::Utf8
    }
}

impl From<String> for Primitive {
    fn from(_: String) -> Primitive {
        Primitive::Utf8
    }
}

impl<'a> From<&'a [u8]> for Primitive {
    fn from(_: &'a [u8]) -> Primitive {
        Primitive::Binary
    }
}
impl<'a> From<Vec<u8>> for Primitive {
    fn from(_: Vec<u8>) -> Primitive {
        Primitive::Binary
    }
}

macro_rules! data_primitive {
    ($name:ident, $( $lifetime:lifetime )?, $String:ty, $Bytes:ty) => {
        #[derive(Debug, Clone)]
        pub enum $name<$($lifetime)?> {
            Boolean(bool),
            Int8(i8),
            Int16(i16),
            Int32(i32),
            Int64(i64),
            UInt8(u8),
            UInt16(u16),
            UInt32(u32),
            UInt64(u64),
            Float32(f32),
            Float64(f64),
            Utf8($String),
            Binary($Bytes),
        }

        impl<$($lifetime)?> From<bool> for $name<$($lifetime)?> {
            fn from(value: bool) -> Self {
                Self::Boolean(value)
            }
        }

        impl<$($lifetime)?> From<u8> for $name<$($lifetime)?> {
            fn from(value: u8) -> Self {
                Self::UInt8(value)
            }
        }

        impl<$($lifetime)?> From<u16> for $name<$($lifetime)?> {
            fn from(value: u16) -> Self {
                Self::UInt16(value)
            }
        }

        impl<$($lifetime)?> From<u32> for $name<$($lifetime)?> {
            fn from(value: u32) -> Self {
                Self::UInt32(value)
            }
        }

        impl<$($lifetime)?> From<u64> for $name<$($lifetime)?> {
            fn from(value: u64) -> Self {
                Self::UInt64(value)
            }
        }
        impl<$($lifetime)?> From<i8> for $name<$($lifetime)?> {
            fn from(value: i8) -> Self {
                Self::Int8(value)
            }
        }

        impl<$($lifetime)?> From<i16> for $name<$($lifetime)?> {
            fn from(value: i16) -> Self {
                Self::Int16(value)
            }
        }

        impl<$($lifetime)?> From<i32> for $name<$($lifetime)?> {
            fn from(value: i32) -> Self {
                Self::Int32(value)
            }
        }

        impl<$($lifetime)?> From<i64> for $name<$($lifetime)?> {
            fn from(value: i64) -> Self {
                Self::Int64(value)
            }
        }


        impl<$($lifetime)?> From<f32> for $name<$($lifetime)?> {
            fn from(value: f32) -> Self {
                Self::Float32(value)
            }
        }


        impl<$($lifetime)?> From<f64> for $name<$($lifetime)?> {
            fn from(value: f64) -> Self {
                Self::Float64(value)
            }
        }

        impl<$($lifetime)?> From<String> for $name<$($lifetime)?> {
            fn from(value: String) -> Self {
                Self::Utf8(value.into())
            }
        }


        impl<$($lifetime)?> From<Vec<u8>> for $name<$($lifetime)?> {
            fn from(value: Vec<u8>) -> Self {
                Self::Binary(value.into())
            }
        }

        impl<$($lifetime)?> From<$name<$($lifetime)?>> for Primitive {
            fn from(value: $name<$($lifetime)?>) -> Primitive {
                match value {
                    $name::Boolean(_) => Primitive::Boolean,
                    $name::Int8(_) => Primitive::Int8,
                    $name::Int16(_) => Primitive::Int16,
                    $name::Int32(_) => Primitive::Int32,
                    $name::Int64(_) => Primitive::Int64,
                    $name::UInt8(_) => Primitive::UInt8,
                    $name::UInt16(_) => Primitive::UInt16,
                    $name::UInt32(_) => Primitive::UInt32,
                    $name::UInt64(_) => Primitive::UInt64,
                    $name::Float32(_) => Primitive::Float32,
                    $name::Float64(_) => Primitive::Float64,
                    $name::Utf8(_) => Primitive::Utf8,
                    $name::Binary(_) => Primitive::Binary,
                }
            }
        }
    };
}

data_primitive!(BorrowedDataPrimitive, 'a, Cow<'a, str>, Cow<'a, [u8]>);

impl<'a> From<&'a str> for BorrowedDataPrimitive<'a> {
    fn from(value: &'a str) -> Self {
        Self::Utf8(value.into())
    }
}

impl<'a> From<&'a [u8]> for BorrowedDataPrimitive<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Binary(value.into())
    }
}

data_primitive!(OwnedDataPrimitive, , String, Bytes);

impl<'a> From<&'a str> for OwnedDataPrimitive {
    fn from(value: &'a str) -> Self {
        Self::Utf8(value.into())
    }
}

impl<'a> From<&'a [u8]> for OwnedDataPrimitive {
    fn from(value: &'a [u8]) -> Self {
        Self::Binary(Bytes::copy_from_slice(value))
    }
}
