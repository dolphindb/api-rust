mod deserialize;
mod serialize;
mod temporal;

use std::{
    fmt::{self, Debug, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

use super::{
    Basic, Bool, Char, DataType, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long,
    Minute, Month, NanoTime, NanoTimeStamp, Second, Short, Time, TimeStamp,
};
use crate::{error::RuntimeError, Deserialize, Serialize};

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

for_all_branches!(dispatch_reflect);

// Scalar trait implementation
pub trait Scalar {
    fn is_null(&self) -> bool;

    // getter methods
    fn get_bool(&self) -> Result<Option<bool>, RuntimeError>;
    fn get_char(&self) -> Result<Option<i8>, RuntimeError>;
    fn get_short(&self) -> Result<Option<i16>, RuntimeError>;
    fn get_int(&self) -> Result<Option<i32>, RuntimeError>;
    fn get_long(&self) -> Result<Option<i64>, RuntimeError>;
    fn get_float(&self) -> Result<Option<f32>, RuntimeError>;
    fn get_double(&self) -> Result<Option<f64>, RuntimeError>;
    fn get_string(&self) -> Result<Option<&str>, RuntimeError>;

    // setter methods
    fn set_bool(&mut self, val: Option<bool>) -> Result<(), RuntimeError>;
    fn set_char(&mut self, val: Option<i8>) -> Result<(), RuntimeError>;
    fn set_short(&mut self, val: Option<i16>) -> Result<(), RuntimeError>;
    fn set_int(&mut self, val: Option<i32>) -> Result<(), RuntimeError>;
    fn set_long(&mut self, val: Option<i64>) -> Result<(), RuntimeError>;
    fn set_float(&mut self, val: Option<f32>) -> Result<(), RuntimeError>;
    fn set_double(&mut self, val: Option<f64>) -> Result<(), RuntimeError>;
    fn set_string(&mut self, val: Option<String>) -> Result<(), RuntimeError>;

    // convert ScalarKind reference
    fn as_bool(&self) -> Result<&Bool, RuntimeError>;
    fn as_char(&self) -> Result<&Char, RuntimeError>;
    fn as_short(&self) -> Result<&Short, RuntimeError>;
    fn as_int(&self) -> Result<&Int, RuntimeError>;
    fn as_long(&self) -> Result<&Long, RuntimeError>;
    fn as_float(&self) -> Result<&Float, RuntimeError>;
    fn as_double(&self) -> Result<&Double, RuntimeError>;
    fn as_string(&self) -> Result<&DolphinString, RuntimeError>;

    // convert ScalarKind mutable reference
    fn as_bool_mut(&mut self) -> Result<&mut Bool, RuntimeError>;
    fn as_char_mut(&mut self) -> Result<&mut Char, RuntimeError>;
    fn as_short_mut(&mut self) -> Result<&mut Short, RuntimeError>;
    fn as_int_mut(&mut self) -> Result<&mut Int, RuntimeError>;
    fn as_long_mut(&mut self) -> Result<&mut Long, RuntimeError>;
    fn as_float_mut(&mut self) -> Result<&mut Float, RuntimeError>;
    fn as_double_mut(&mut self) -> Result<&mut Double, RuntimeError>;
    fn as_string_mut(&mut self) -> Result<&mut DolphinString, RuntimeError>;
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

    // implement getter methods
    fn get_bool(&self) -> Result<Option<bool>, RuntimeError> {
        if let ScalarKind::Bool(obj) = self {
            Ok(obj.get_bool())
        } else {
            Err(RuntimeError::NotBoolScalar)
        }
    }
    fn get_char(&self) -> Result<Option<i8>, RuntimeError> {
        if let ScalarKind::Char(obj) = self {
            Ok(obj.get_char())
        } else {
            Err(RuntimeError::NotCharScalar)
        }
    }
    fn get_short(&self) -> Result<Option<i16>, RuntimeError> {
        if let ScalarKind::Short(obj) = self {
            Ok(obj.get_short())
        } else {
            Err(RuntimeError::NotShortScalar)
        }
    }
    fn get_int(&self) -> Result<Option<i32>, RuntimeError> {
        match self {
            ScalarKind::Int(obj) => Ok(obj.get_int()),
            ScalarKind::Date(obj) => Ok(obj.get_int()),
            ScalarKind::Month(obj) => Ok(obj.get_int()),
            ScalarKind::Time(obj) => Ok(obj.get_int()),
            ScalarKind::Minute(obj) => Ok(obj.get_int()),
            ScalarKind::Second(obj) => Ok(obj.get_int()),
            ScalarKind::DateTime(obj) => Ok(obj.get_int()),
            ScalarKind::DateHour(obj) => Ok(obj.get_int()),
            _ => Err(RuntimeError::NotIntNorTemporal32Scalar),
        }
    }
    fn get_long(&self) -> Result<Option<i64>, RuntimeError> {
        match self {
            ScalarKind::Long(obj) => Ok(obj.get_long()),
            ScalarKind::TimeStamp(obj) => Ok(obj.get_long()),
            ScalarKind::NanoTime(obj) => Ok(obj.get_long()),
            ScalarKind::NanoTimeStamp(obj) => Ok(obj.get_long()),
            _ => Err(RuntimeError::NotLongNorTemporal64Scalar),
        }
    }
    fn get_float(&self) -> Result<Option<f32>, RuntimeError> {
        if let ScalarKind::Float(obj) = self {
            Ok(obj.get_float())
        } else {
            Err(RuntimeError::NotFloatScalar)
        }
    }
    fn get_double(&self) -> Result<Option<f64>, RuntimeError> {
        if let ScalarKind::Double(obj) = self {
            Ok(obj.get_double())
        } else {
            Err(RuntimeError::NotDoubleScalar)
        }
    }
    fn get_string(&self) -> Result<Option<&str>, RuntimeError> {
        if let ScalarKind::String(obj) = self {
            Ok(obj.get_string())
        } else {
            Err(RuntimeError::NotStringScalar)
        }
    }

    // implement setter methods
    fn set_bool(&mut self, val: Option<bool>) -> Result<(), RuntimeError> {
        if let ScalarKind::Bool(obj) = self {
            obj.set_bool(val);
            Ok(())
        } else {
            Err(RuntimeError::NotBoolScalar)
        }
    }
    fn set_char(&mut self, val: Option<i8>) -> Result<(), RuntimeError> {
        if let ScalarKind::Char(obj) = self {
            obj.set_char(val);
            Ok(())
        } else {
            Err(RuntimeError::NotCharScalar)
        }
    }
    fn set_short(&mut self, val: Option<i16>) -> Result<(), RuntimeError> {
        if let ScalarKind::Short(obj) = self {
            obj.set_short(val);
            Ok(())
        } else {
            Err(RuntimeError::NotShortScalar)
        }
    }
    fn set_int(&mut self, val: Option<i32>) -> Result<(), RuntimeError> {
        match self {
            ScalarKind::Int(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::Date(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::Month(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::Time(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::Minute(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::Second(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::DateTime(obj) => {
                obj.set_int(val);
                Ok(())
            }
            ScalarKind::DateHour(obj) => {
                obj.set_int(val);
                Ok(())
            }
            _ => Err(RuntimeError::NotIntNorTemporal32Scalar),
        }
    }
    fn set_long(&mut self, val: Option<i64>) -> Result<(), RuntimeError> {
        match self {
            ScalarKind::Long(obj) => {
                obj.set_long(val);
                Ok(())
            }
            ScalarKind::TimeStamp(obj) => {
                obj.set_long(val);
                Ok(())
            }
            ScalarKind::NanoTime(obj) => {
                obj.set_long(val);
                Ok(())
            }
            ScalarKind::NanoTimeStamp(obj) => {
                obj.set_long(val);
                Ok(())
            }
            _ => Err(RuntimeError::NotLongNorTemporal64Scalar),
        }
    }
    fn set_float(&mut self, val: Option<f32>) -> Result<(), RuntimeError> {
        if let ScalarKind::Float(obj) = self {
            obj.set_float(val);
            Ok(())
        } else {
            Err(RuntimeError::NotFloatScalar)
        }
    }
    fn set_double(&mut self, val: Option<f64>) -> Result<(), RuntimeError> {
        if let ScalarKind::Double(obj) = self {
            obj.set_double(val);
            Ok(())
        } else {
            Err(RuntimeError::NotDoubleScalar)
        }
    }
    fn set_string(&mut self, val: Option<String>) -> Result<(), RuntimeError> {
        if let ScalarKind::String(obj) = self {
            obj.set_string(val);
            Ok(())
        } else {
            Err(RuntimeError::NotStringScalar)
        }
    }

    // convert ScalarKind reference
    fn as_bool(&self) -> Result<&Bool, RuntimeError> {
        match self {
            Self::Bool(obj) => Ok(obj),
            _ => Err(RuntimeError::NotBoolScalar),
        }
    }
    fn as_char(&self) -> Result<&Char, RuntimeError> {
        match self {
            Self::Char(obj) => Ok(obj),
            _ => Err(RuntimeError::NotCharScalar),
        }
    }
    fn as_short(&self) -> Result<&Short, RuntimeError> {
        match self {
            Self::Short(obj) => Ok(obj),
            _ => Err(RuntimeError::NotShortScalar),
        }
    }
    fn as_int(&self) -> Result<&Int, RuntimeError> {
        match self {
            Self::Int(obj) => Ok(obj),
            _ => Err(RuntimeError::NotIntScalar),
        }
    }
    fn as_long(&self) -> Result<&Long, RuntimeError> {
        match self {
            Self::Long(obj) => Ok(obj),
            _ => Err(RuntimeError::NotLongScalar),
        }
    }
    fn as_float(&self) -> Result<&Float, RuntimeError> {
        match self {
            Self::Float(obj) => Ok(obj),
            _ => Err(RuntimeError::NotFloatScalar),
        }
    }
    fn as_double(&self) -> Result<&Double, RuntimeError> {
        match self {
            Self::Double(obj) => Ok(obj),
            _ => Err(RuntimeError::NotDoubleScalar),
        }
    }
    fn as_string(&self) -> Result<&DolphinString, RuntimeError> {
        match self {
            Self::String(obj) => Ok(obj),
            _ => Err(RuntimeError::NotStringScalar),
        }
    }

    // convert ScalarKind mutable reference
    fn as_bool_mut(&mut self) -> Result<&mut Bool, RuntimeError> {
        match self {
            Self::Bool(obj) => Ok(obj),
            _ => Err(RuntimeError::NotBoolScalar),
        }
    }
    fn as_char_mut(&mut self) -> Result<&mut Char, RuntimeError> {
        match self {
            Self::Char(obj) => Ok(obj),
            _ => Err(RuntimeError::NotCharScalar),
        }
    }
    fn as_short_mut(&mut self) -> Result<&mut Short, RuntimeError> {
        match self {
            Self::Short(obj) => Ok(obj),
            _ => Err(RuntimeError::NotShortScalar),
        }
    }
    fn as_int_mut(&mut self) -> Result<&mut Int, RuntimeError> {
        match self {
            Self::Int(obj) => Ok(obj),
            _ => Err(RuntimeError::NotIntScalar),
        }
    }
    fn as_long_mut(&mut self) -> Result<&mut Long, RuntimeError> {
        match self {
            Self::Long(obj) => Ok(obj),
            _ => Err(RuntimeError::NotLongScalar),
        }
    }
    fn as_float_mut(&mut self) -> Result<&mut Float, RuntimeError> {
        match self {
            Self::Float(obj) => Ok(obj),
            _ => Err(RuntimeError::NotFloatScalar),
        }
    }
    fn as_double_mut(&mut self) -> Result<&mut Double, RuntimeError> {
        match self {
            Self::Double(obj) => Ok(obj),
            _ => Err(RuntimeError::NotDoubleScalar),
        }
    }
    fn as_string_mut(&mut self) -> Result<&mut DolphinString, RuntimeError> {
        match self {
            Self::String(obj) => Ok(obj),
            _ => Err(RuntimeError::NotStringScalar),
        }
    }
}

// implement Display trait
impl Display for ScalarKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarKind::Void => write!(f, ""),
            ScalarKind::Bool(obj) => <Bool as Display>::fmt(obj, f),
            ScalarKind::Char(obj) => <Char as Display>::fmt(obj, f),
            ScalarKind::Short(obj) => <Short as Display>::fmt(obj, f),
            ScalarKind::Int(obj) => <Int as Display>::fmt(obj, f),
            ScalarKind::Long(obj) => <Long as Display>::fmt(obj, f),
            ScalarKind::Date(obj) => <Date as Display>::fmt(obj, f),
            ScalarKind::Month(obj) => <Month as Display>::fmt(obj, f),
            ScalarKind::Time(obj) => <Time as Display>::fmt(obj, f),
            ScalarKind::Minute(obj) => <Minute as Display>::fmt(obj, f),
            ScalarKind::Second(obj) => <Second as Display>::fmt(obj, f),
            ScalarKind::DateTime(obj) => <DateTime as Display>::fmt(obj, f),
            ScalarKind::TimeStamp(obj) => <TimeStamp as Display>::fmt(obj, f),
            ScalarKind::NanoTime(obj) => <NanoTime as Display>::fmt(obj, f),
            ScalarKind::NanoTimeStamp(obj) => <NanoTimeStamp as Display>::fmt(obj, f),
            ScalarKind::Float(obj) => <Float as Display>::fmt(obj, f),
            ScalarKind::Double(obj) => <Double as Display>::fmt(obj, f),
            ScalarKind::String(obj) => <DolphinString as Display>::fmt(obj, f),
            ScalarKind::DateHour(obj) => <DateHour as Display>::fmt(obj, f),
        }
    }
}
