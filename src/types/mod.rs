//! DolphinDB type wrappers to make our lives easier when interacting with the Dlang type system.
//!
//! See [DolphinDB docs](https://docs.dolphindb.cn/zh/rustdoc/chap2_dataobjects_landingpage.html) for more information.

mod any;
mod array_vector;
mod constant;
mod decimal;
mod dictionary;
mod enums;
mod pair;
mod primitive;
mod scalar;
mod set;
mod table;
mod temporal;
mod vector;

use std::any::type_name;
use std::fmt::{self, Display};

use crate::error::Error;
pub use any::*;
pub use array_vector::*;
pub use constant::*;
pub use decimal::*;
pub use dictionary::*;
pub use enums::*;
pub use pair::*;
pub use primitive::*;
pub use scalar::*;
pub use set::*;
pub use table::*;
pub use temporal::*;
pub use vector::*;

macro_rules! to_scalar_impl {
    (DolphinString, DolphinString) => {
        to_scalar_impl!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<$struct_name> for ScalarImpl {
            fn from(value: $struct_name) -> Self {
                Self::$enum_name(value)
            }
        }

        impl TryFrom<ScalarImpl> for $struct_name {
            type Error = Error;

            fn try_from(value: ScalarImpl) -> Result<Self, Self::Error> {
                match value {
                    ScalarImpl::$enum_name(value) => Ok(value),
                    _ => Err(Error::InvalidConvert {
                        from: type_name::<ScalarImpl>().to_string(),
                        to: type_name::<$struct_name>().to_string(),
                    }),
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            to_scalar_impl!($struct_name, $struct_name);
        )*
    };
}

macro_rules! display_impl {
    (Void) => {};

    (Blob) => {};

    (Char) => {};

    ($struct_name:ident) => {
        impl Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if let Some(val) = self.as_ref() {
                    write!(f, "{}", val)
                } else {
                    write!(f, "null")
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            display_impl!($struct_name);
        )*
    };
}

impl Display for Void {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "void")
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(val) = self.as_ref() {
            write!(f, "{:?}", val)
        } else {
            write!(f, "null")
        }
    }
}

impl Display for Char {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(val) = self.as_ref() {
            write!(f, "{}", *val as u8 as char)
        } else {
            write!(f, "null")
        }
    }
}

macro_rules! scalar_trait_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl Scalar for $struct_name {
            fn data_type() -> DataType {
                Self::DATA_BYTE
            }

            fn is_null(&self) -> bool {
                self.0.is_none()
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            scalar_trait_impl!($raw_type, $struct_name);
        )*
    };
}

pub trait NotDecimal {}

macro_rules! non_decimal_marker {
    (Decimal32) => {};

    (Decimal64) => {};

    (Decimal128) => {};

    ($struct_name:ident) => {
        impl NotDecimal for $struct_name {}
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            non_decimal_marker!($struct_name);
        )*
    };
}

impl NotDecimal for Any {}

macro_rules! to_constant_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<$struct_name> for ConstantImpl {
            fn from(value: $struct_name) -> Self {
                let s: ScalarImpl = value.into();
                s.into()
            }
        }

        impl TryFrom<ConstantImpl> for $struct_name {
            type Error = Error;

            fn try_from(value: ConstantImpl) -> Result<Self, Self::Error> {
                let s: ScalarImpl = value.try_into()?;
                s.try_into()
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            to_constant_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! to_any_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<$struct_name> for Any {
            fn from(value: $struct_name) -> Self {
                let s: ScalarImpl = value.into();
                s.into()
            }
        }

        impl TryFrom<Any> for $struct_name {
            type Error = Error;

            fn try_from(value: Any) -> Result<Self, Self::Error> {
                let c: ConstantImpl = value.into_inner();
                c.try_into()
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            to_any_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! for_all_types {
    ($macro:tt) => {
        $macro!(
            ((), Void),
            (bool, Bool),
            (NaiveDate, Date),
            (NaiveDate, Month),
            (NaiveTime, Time),
            (NaiveTime, Minute),
            (NaiveTime, Second),
            (NaiveDateTime, DateTime),
            (NaiveDateTime, Timestamp),
            (NaiveTime, NanoTime),
            (NaiveDateTime, NanoTimestamp),
            (String, Symbol),
            (String, DolphinString),
            (U8Vec, Blob),
            (NaiveDateTime, DateHour),
            (Decimal, Decimal32),
            (Decimal, Decimal64),
            (Decimal, Decimal128),
            (u8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (f32, Float),
            (f64, Double)
        );
    };
}

pub(crate) use for_all_types;

for_all_types!(to_scalar_impl);

for_all_types!(to_constant_impl);

for_all_types!(to_any_impl);

for_all_types!(scalar_trait_impl);

macro_rules! for_all_display_types {
    ($macro:tt) => {
        $macro!(
            ((), Void),
            (bool, Bool),
            (NaiveTime, Time),
            (NaiveTime, Minute),
            (NaiveTime, Second),
            (String, Symbol),
            (String, DolphinString),
            (U8Vec, Blob),
            (Decimal, Decimal32),
            (Decimal, Decimal64),
            (Decimal, Decimal128),
            (u8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (f32, Float),
            (f64, Double)
        );
    };
}

for_all_display_types!(display_impl);

for_all_types!(non_decimal_marker);
