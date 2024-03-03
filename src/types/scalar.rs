mod deserialize;
mod serialize;
mod temporal;

use super::{
    Basic, Bool, Char, Constant, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long,
    Minute, Month, NanoTime, NanoTimeStamp, Second, Short, Time, TimeStamp,
};
use crate::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

// TODO: delete DataByte
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum DataType {
    Void,
    Bool,
    Char,
    Short,
    Int,
    Long,
    Date,
    Month,
    Time,
    Minute,
    Second,
    DateTime,
    TimeStamp,
    NanoTime,
    NanoTimeStamp,
    Float,
    Double,
    Placeholder1,
    DolphinString, // todo
    Placeholder2,
    Placeholder3,
    Placeholder4,
    Placeholder5,
    Placeholder6,
    Placeholder7,
    Any,
    Placeholder8,
    Placeholder9,
    DateHour,
}

// todo: use From or TryFrom trait
impl DataType {
    pub fn from_u8(data_type: u8) -> Option<DataType> {
        match data_type {
            0 => Some(DataType::Void),
            1 => Some(DataType::Bool),
            2 => Some(DataType::Char),
            3 => Some(DataType::Short),
            4 => Some(DataType::Int),
            5 => Some(DataType::Long),
            6 => Some(DataType::Date),
            7 => Some(DataType::Month),
            8 => Some(DataType::Time),
            9 => Some(DataType::Minute),
            10 => Some(DataType::Second),
            11 => Some(DataType::DateTime),
            12 => Some(DataType::TimeStamp),
            13 => Some(DataType::NanoTime),
            14 => Some(DataType::NanoTimeStamp),
            15 => Some(DataType::Float),
            16 => Some(DataType::Double),
            17 => Some(DataType::Placeholder1),
            18 => Some(DataType::DolphinString),
            19 => Some(DataType::Placeholder2),
            20 => Some(DataType::Placeholder3),
            21 => Some(DataType::Placeholder4),
            22 => Some(DataType::Placeholder5),
            23 => Some(DataType::Placeholder6),
            24 => Some(DataType::Placeholder7),
            25 => Some(DataType::Any),
            26 => Some(DataType::Placeholder8),
            27 => Some(DataType::Placeholder9),
            28 => Some(DataType::DateHour),
            _ => None,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            DataType::Void => 0,
            DataType::Bool => 1,
            DataType::Char => 2,
            DataType::Short => 3,
            DataType::Int => 4,
            DataType::Long => 5,
            DataType::Date => 6,
            DataType::Month => 7,
            DataType::Time => 8,
            DataType::Minute => 9,
            DataType::Second => 10,
            DataType::DateTime => 11,
            DataType::TimeStamp => 12,
            DataType::NanoTime => 13,
            DataType::NanoTimeStamp => 14,
            DataType::Float => 15,
            DataType::Double => 16,
            DataType::Placeholder1 => 17,
            DataType::DolphinString => 18,
            DataType::Placeholder2 => 19,
            DataType::Placeholder3 => 20,
            DataType::Placeholder4 => 21,
            DataType::Placeholder5 => 22,
            DataType::Placeholder6 => 23,
            DataType::Placeholder7 => 24,
            DataType::Any => 25,
            DataType::Placeholder8 => 26,
            DataType::Placeholder9 => 27,
            DataType::DateHour => 28,
        }
    }
}

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
                (self.data_type().to_u8(), self.data_category()).serialize(buffer)?;

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
                (self.data_type().to_u8(), self.data_category()).serialize_le(buffer)?;

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

// Scalar trait implementation
pub trait Scalar: Basic {
    type RawType: Send + Sync + Clone;
    type RefType<'a>: Send + Copy;

    fn new(raw: Self::RawType) -> Self;
    fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType;
    fn data_type() -> DataType;
}

impl Scalar for () {
    type RawType = ();
    type RefType<'a> = ();

    fn new(_: Self::RawType) -> Self {}
    fn to_owned(_: Self::RefType<'_>) -> Self::RawType {}

    fn data_type() -> DataType {
        DataType::Void
    }
}
