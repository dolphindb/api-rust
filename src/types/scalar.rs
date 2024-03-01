mod deserialize;
mod serialize;
mod temporal;

use super::constant::Constant;
pub use super::*;
use crate::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

pub const ANY_TYPE_VALUE: u8 = 25;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum ScalarKind {
    Void,
    Bool(Bool),
    Char(Char),
    Short(Short),
    Int(Int),
    Long(Long),
    Date(Date),
    Month(Month),
    Time(Time),
    Minute(Minute),
    Second(Second),
    DateTime(DateTime),
    TimeStamp(TimeStamp),
    NanoTime(NanoTime),
    NanoTimeStamp(NanoTimeStamp),
    Float(Float),
    Double(Double),
    String(DolphinString),
    DateHour(DateHour),
}

impl Default for ScalarKind {
    fn default() -> Self {
        Self::Void
    }
}

impl From<()> for ScalarKind {
    fn from(_: ()) -> Self {
        Self::Void
    }
}

impl TryFrom<ScalarKind> for () {
    type Error = ();

    fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
        match value {
            ScalarKind::Void => Ok(()),
            _ => Err(()),
        }
    }
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Serialize for ScalarKind {
            fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
            where
                B: bytes::BufMut,
            {
                (self.data_type(), self.data_category()).serialize(buffer)?;

                match self {
                    Self::Void => ().serialize(buffer),
                    $(
                        Self::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
            where
                B: bytes::BufMut,
            {
                (self.data_type(), self.data_category()).serialize_le(buffer)?;

                match self {
                    Self::Void => ().serialize_le(buffer),
                    $(
                        Self::$enum_name(s) => s.serialize_le(buffer),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_deserialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Deserialize for ScalarKind {
            async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    Self::Void => ().deserialize(reader).await,
                    $(
                        Self::$enum_name(s) => s.deserialize(reader).await,
                    )*
                }
            }

            async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    Self::Void => ().deserialize_le(reader).await,
                    $(
                        Self::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for ScalarKind {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    ScalarKind::Void => write!(f, ""),
                    $(
                        ScalarKind::$enum_name(s) => write!(f, "{}", s),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl ScalarKind {
            pub(crate) fn from_type(data_type: u8) -> Option<Self> {
                match data_type {
                    0 => Some(Self::Void),
                    $(
                        $struct_name::DATA_TYPE => Some(Self::$enum_name($struct_name::default())),
                    )*
                    _ => None,
                }
            }
        }
    };
}

macro_rules! for_all_branches {
    ($macro:tt) => {
        $macro!(
            (Bool, Bool),
            (Date, Date),
            (Month, Month),
            (Time, Time),
            (Minute, Minute),
            (Second, Second),
            (DateTime, DateTime),
            (TimeStamp, TimeStamp),
            (NanoTime, NanoTime),
            (NanoTimeStamp, NanoTimeStamp),
            (String, DolphinString),
            (DateHour, DateHour),
            (Char, Char),
            (Short, Short),
            (Int, Int),
            (Long, Long),
            (Float, Float),
            (Double, Double)
        );
    };
}

pub(crate) use for_all_branches;

for_all_branches!(dispatch_serialize);

for_all_branches!(dispatch_deserialize);

for_all_branches!(dispatch_display);

for_all_branches!(dispatch_reflect);

impl ScalarKind {
    pub const FORM_BYTE: u8 = 0;

    pub const fn data_form(&self) -> u8 {
        Self::FORM_BYTE
    }
}

impl Constant for ScalarKind {
    fn data_category(&self) -> u8 {
        Self::FORM_BYTE
    }

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }
}

// Basic trait implementation
pub trait Basic: Send + Sync + Clone {
    fn data_type(&self) -> u8;
    fn is_null(&self) -> bool {
        false
    }

    // default implementation of Basic getters
    fn get_bool(&self) -> Result<bool, RuntimeError> {
        Err(RuntimeError::GetBoolFail)
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        Err(RuntimeError::GetCharFail)
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        Err(RuntimeError::GetShortFail)
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        Err(RuntimeError::GetIntFail)
    }
}

// implement Basic trait for ScalarKind
impl Basic for ScalarKind {
    fn data_type(&self) -> u8 {
        match self {
            ScalarKind::Void => 0,
            ScalarKind::Bool(obj) => obj.data_type(),
            ScalarKind::Char(obj) => obj.data_type(),
            ScalarKind::Short(obj) => obj.data_type(),
            ScalarKind::Int(obj) => obj.data_type(),
            ScalarKind::Long(obj) => obj.data_type(),
            ScalarKind::Date(obj) => obj.data_type(),
            ScalarKind::Month(obj) => obj.data_type(),
            ScalarKind::Time(obj) => obj.data_type(),
            ScalarKind::Minute(obj) => obj.data_type(),
            ScalarKind::Second(obj) => obj.data_type(),
            ScalarKind::DateTime(obj) => obj.data_type(),
            ScalarKind::TimeStamp(obj) => obj.data_type(),
            ScalarKind::NanoTime(obj) => obj.data_type(),
            ScalarKind::NanoTimeStamp(obj) => obj.data_type(),
            ScalarKind::Float(obj) => obj.data_type(),
            ScalarKind::Double(obj) => obj.data_type(),
            ScalarKind::String(obj) => obj.data_type(),
            ScalarKind::DateHour(obj) => obj.data_type(),
        }
    }

    fn is_null(&self) -> bool {
        match self {
            ScalarKind::Void => true,
            ScalarKind::Bool(obj) => obj.is_null(),
            ScalarKind::Char(obj) => obj.is_null(),
            ScalarKind::Short(obj) => obj.is_null(),
            ScalarKind::Int(obj) => obj.is_null(),
            ScalarKind::Long(obj) => obj.is_null(),
            ScalarKind::Date(obj) => obj.is_null(),
            ScalarKind::Month(obj) => obj.is_null(),
            ScalarKind::Time(obj) => obj.is_null(),
            ScalarKind::Minute(obj) => obj.is_null(),
            ScalarKind::Second(obj) => obj.is_null(),
            ScalarKind::DateTime(obj) => obj.is_null(),
            ScalarKind::TimeStamp(obj) => obj.is_null(),
            ScalarKind::NanoTime(obj) => obj.is_null(),
            ScalarKind::NanoTimeStamp(obj) => obj.is_null(),
            ScalarKind::Float(obj) => obj.is_null(),
            ScalarKind::Double(obj) => obj.is_null(),
            ScalarKind::String(obj) => obj.is_null(),
            ScalarKind::DateHour(obj) => obj.is_null(),
        }
    }

    // implementation of Basic getters
    fn get_bool(&self) -> Result<bool, RuntimeError> {
        match self {
            ScalarKind::Bool(obj) => obj.get_bool(),
            _ => Err(RuntimeError::GetBoolFail),
        }
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        match self {
            ScalarKind::Char(obj) => obj.get_char(),
            _ => Err(RuntimeError::GetCharFail),
        }
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        match self {
            ScalarKind::Short(obj) => obj.get_short(),
            _ => Err(RuntimeError::GetShortFail),
        }
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        match self {
            ScalarKind::Int(obj) => obj.get_int(),
            _ => Err(RuntimeError::GetIntFail),
        }
    }
}

// Scalar trait implementation
pub trait Scalar: Basic {
    type RawType: Send + Sync + Clone;
    type RefType<'a>: Send + Copy;

    fn new(raw: Self::RawType) -> Self;
    fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType;
    fn data_type() -> u8;
}
