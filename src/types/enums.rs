use std::fmt::Display;

use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum DataType {
    #[default]
    Void = 0,
    Bool = 1,
    Char = 2,
    Short = 3,
    Int = 4,
    Long = 5,
    Date = 6,
    Month = 7,
    Time = 8,
    Minute = 9,
    Second = 10,
    DateTime = 11,
    Timestamp = 12,
    NanoTime = 13,
    NanoTimestamp = 14,
    Float = 15,
    Double = 16,
    Symbol = 17,
    String = 18,
    Any = 25,
    AnyDictionary = 27,
    DateHour = 28,
    Blob = 32,
    Decimal32 = 37,
    Decimal64 = 38,
    Decimal128 = 39,

    CharArray = 66,
    ShortArray = 67,
    IntArray = 68,
    LongArray = 69,
    FloatArray = 79,
    DoubleArray = 80,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum DataForm {
    Scalar,
    Vector,
    Pair,
    Matrix,
    Set,
    Dictionary,
    Table,
}

macro_rules! data_type_display {
    ($(($enum_name:ident, $repr:tt)), *) => {
        impl Display for DataType {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        Self::$enum_name => write!(f, "{}", stringify!($enum_name)),
                    )*
                }
            }
        }
    };
}

macro_rules! data_type_from_u8 {
    ($(($enum_name:ident, $repr:tt)), *) => {
        impl TryFrom<u8> for DataType {
            type Error = Error;

            fn try_from(value: u8) -> Result<Self> {
                match value {
                    $(
                        $repr => Ok(Self::$enum_name),
                    )*
                    _ => panic!("Unsupported data type"),
                }
            }
        }
    };
}

macro_rules! for_all_types {
    ($macro:tt) => {
        $macro!(
            (Void, 0),
            (Bool, 1),
            (Char, 2),
            (Short, 3),
            (Int, 4),
            (Long, 5),
            (Date, 6),
            (Month, 7),
            (Time, 8),
            (Minute, 9),
            (Second, 10),
            (DateTime, 11),
            (Timestamp, 12),
            (NanoTime, 13),
            (NanoTimestamp, 14),
            (Float, 15),
            (Double, 16),
            (Symbol, 17),
            (String, 18),
            (Any, 25),
            (AnyDictionary, 27),
            (DateHour, 28),
            (Blob, 32),
            (Decimal32, 37),
            (Decimal64, 38),
            (Decimal128, 39),
            (CharArray, 66),
            (ShortArray, 67),
            (IntArray, 68),
            (LongArray, 69),
            (FloatArray, 79),
            (DoubleArray, 80)
        );
    };
}

for_all_types!(data_type_display);

for_all_types!(data_type_from_u8);

macro_rules! data_form_display {
    ($(($enum_name:ident, $repr:tt)), *) => {
        impl Display for DataForm {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        Self::$enum_name => write!(f, "{}", stringify!($enum_name)),
                    )*
                }
            }
        }
    };
}

macro_rules! data_form_from_u8 {
    ($(($enum_name:ident, $repr:tt)), *) => {
        impl TryFrom<u8> for DataForm {
            type Error = Error;

            fn try_from(value: u8) -> Result<Self> {
                match value {
                    $(
                        $repr => Ok(Self::$enum_name),
                    )*
                    _ => Err(Error::InvalidConvert {
                        from: value.to_string(),
                        to: stringify!(DataForm).to_string(),
                    }),
                }
            }
        }
    };
}

macro_rules! for_all_forms {
    ($macro:tt) => {
        $macro!(
            (Scalar, 0),
            (Vector, 1),
            (Pair, 2),
            (Matrix, 3),
            (Set, 4),
            (Dictionary, 5),
            (Table, 6)
        );
    };
}

for_all_forms!(data_form_display);

for_all_forms!(data_form_from_u8);

impl Serialize for (DataType, DataForm) {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        (self.0 as u8, self.1 as u8).serialize(buffer)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        (self.0 as u8, self.1 as u8).serialize_le(buffer)
    }
}

impl Deserialize for (DataType, DataForm) {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: tokio::io::AsyncBufReadExt + Unpin,
    {
        (self.0 as u8, self.1 as u8).deserialize(reader).await
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: tokio::io::AsyncBufReadExt + Unpin,
    {
        (self.0 as u8, self.1 as u8).deserialize_le(reader).await
    }
}
