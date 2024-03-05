mod deserialize;
mod serialize;
mod temporal;

use super::{
    Basic, Bool, Char, ConcreteScalar, DataType, Date, DateHour, DateTime, DolphinString, Double,
    Float, Int, Long, Minute, Month, NanoTime, NanoTimeStamp, Second, Short, Time, TimeStamp,
};
use crate::{error::RuntimeError, Deserialize, Serialize};
use std::{
    fmt::{self, Debug, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

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
    type Error = RuntimeError;

    fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
        match value {
            ScalarKind::Void => Ok(()),
            _ => Err(RuntimeError::ConvertFail),
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
                (self.data_type().to_u8(), self.data_form().to_u8()).serialize(buffer)?;

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
                (self.data_type().to_u8(), self.data_form().to_u8()).serialize_le(buffer)?;

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
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    DataType::Void => Some(Self::Void),
                    $(
                        DataType::$struct_name => Some(Self::$enum_name($struct_name::default())),
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

// Scalar trait implementation
pub trait Scalar {
    fn is_null(&self) -> bool;

    fn get_bool(&self) -> Result<Option<bool>, RuntimeError>;
    fn get_char(&self) -> Result<Option<u8>, RuntimeError>;
    fn get_short(&self) -> Result<Option<i16>, RuntimeError>;
    fn get_int(&self) -> Result<Option<i32>, RuntimeError>;
    fn get_long(&self) -> Result<Option<i64>, RuntimeError>;
    fn get_float(&self) -> Result<Option<f32>, RuntimeError>;
    fn get_double(&self) -> Result<Option<f64>, RuntimeError>;
    fn get_string(&self) -> Result<Option<&str>, RuntimeError>;

    // 3. set
    // 4. get_$rawtype
    // 5. set_$rawtype
}

impl Scalar for ScalarKind {
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

    // todo implement getter methods
    fn get_bool(&self) -> Result<Option<bool>, RuntimeError> {
        match self {
            ScalarKind::Bool(obj) => Ok(obj.get_bool()),
            _ => Err(RuntimeError::NotBoolScalar),
        }
    }
    fn get_char(&self) -> Result<Option<u8>, RuntimeError> {
        match self {
            ScalarKind::Char(obj) => Ok(obj.get_char()),
            _ => Err(RuntimeError::NotCharScalar),
        }
    }
    fn get_short(&self) -> Result<Option<i16>, RuntimeError> {
        match self {
            ScalarKind::Short(obj) => Ok(obj.get_short()),
            _ => Err(RuntimeError::NotShortScalar),
        }
    }
    fn get_int(&self) -> Result<Option<i32>, RuntimeError> {
        match self {
            ScalarKind::Int(obj) => Ok(obj.get_int()),
            _ => Err(RuntimeError::NotIntScalar),
        }
    }
    fn get_long(&self) -> Result<Option<i64>, RuntimeError> {
        match self {
            ScalarKind::Long(obj) => Ok(obj.get_long()),
            _ => Err(RuntimeError::NotLongScalar),
        }
    }
    fn get_float(&self) -> Result<Option<f32>, RuntimeError> {
        match self {
            ScalarKind::Float(obj) => Ok(obj.get_float()),
            _ => Err(RuntimeError::NotFloatScalar),
        }
    }
    fn get_double(&self) -> Result<Option<f64>, RuntimeError> {
        match self {
            ScalarKind::Double(obj) => Ok(obj.get_double()),
            _ => Err(RuntimeError::NotDoubleScalar),
        }
    }
    fn get_string(&self) -> Result<Option<&str>, RuntimeError> {
        match self {
            ScalarKind::String(obj) => Ok(obj.get_string()),
            _ => Err(RuntimeError::NotStringScalar),
        }
    }
}
