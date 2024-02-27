mod decimal;
mod deserialize;
mod serialize;
mod temporal;

use super::constant::Constant;
pub use super::*;
use crate::{Deserialize, Serialize};
pub use decimal::DecimalInterface;
use std::{
    fmt::{self, Debug, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

pub trait Scalar: Send + Sync + Clone + Debug + Default + PartialEq + PartialOrd + Hash {
    type RawType: Send + Sync + Clone;

    type RefType<'a>: Send + Copy;

    fn new(raw: Self::RawType) -> Self;

    fn data_type() -> u8;

    fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType;

    fn is_null(&self) -> bool;
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

    Decimal32(Decimal32),
    Decimal64(Decimal64),
    Decimal128(Decimal128),
}

impl Default for ScalarKind {
    fn default() -> Self {
        Self::Void
    }
}

impl Scalar for () {
    type RawType = ();

    type RefType<'a> = ();

    fn new(_: Self::RawType) -> Self {}

    fn data_type() -> u8 {
        0
    }

    fn to_owned(_: Self::RefType<'_>) -> Self::RawType {}

    fn is_null(&self) -> bool {
        true
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

macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl ScalarKind {
            pub fn data_type(&self) -> u8 {
                match self {
                    ScalarKind::Void => 0,
                    $(
                        ScalarKind::$enum_name(s) => s.data_type(),
                    )*
                }
            }
        }
    };
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
                        $struct_name::DATA_BYTE => Some(Self::$enum_name($struct_name::default())),
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
            (Decimal32, Decimal32),
            (Decimal64, Decimal64),
            (Decimal128, Decimal128),
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

for_all_branches!(dispatch_data_type);

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
