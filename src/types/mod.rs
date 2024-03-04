mod basic;
mod constant;
mod dictionary;
mod pair;
mod scalar;
mod set;
mod vector;

pub use basic::{Basic, DataCategory, DataForm, DataType};
pub use constant::ConstantKind;
pub use dictionary::Dictionary;
pub use pair::Pair;
pub use scalar::{Scalar, ScalarKind};
pub use set::Set;
pub use vector::VectorKind;

use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use ordered_float::OrderedFloat;
use std::fmt::{self, Debug, Display};

use crate::error::RuntimeError;

type OrderedFloatF32 = OrderedFloat<f32>;
type OrderedFloatF64 = OrderedFloat<f64>;

// helper iterator macro
macro_rules! for_all_scalars {
    ($macro:tt) => {
        $macro!(
            (bool, Bool),
            (NaiveDate, Date),
            (NaiveDate, Month),
            (NaiveTime, Time),
            (NaiveTime, Minute),
            (NaiveTime, Second),
            (NaiveDateTime, DateTime),
            (NaiveDateTime, TimeStamp),
            (NaiveTime, NanoTime),
            (NaiveDateTime, NanoTimeStamp),
            (String, DolphinString),
            (NaiveDateTime, DateHour),
            (u8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (OrderedFloatF32, Float),
            (OrderedFloatF64, Double)
        );
    };
}

//  implement trivial functions
macro_rules! trivial_impl {
    (String, DolphinString) => {
        #[derive(Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct DolphinString(Option<String>);

        impl DolphinString {
            pub fn new(val: Option<String>) -> Self {
                Self(val)
            }

            pub fn set(&mut self, val: Option<String>) {
                self.0 = val;
            }

            pub fn set_unchecked(&mut self, val: String) {
                self.0 = Some(val)
            }

            pub const fn get(&self) -> &Option<String> {
                &self.0
            }

            pub const fn as_ref(&self) -> Option<&String> {
                self.0.as_ref()
            }

            pub fn get_mut(&mut self) -> &mut Option<String> {
                &mut self.0
            }

            pub fn as_mut(&mut self) -> Option<&mut String> {
                self.0.as_mut()
            }

            pub fn into_inner(self) -> Option<String> {
                self.0
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct $struct_name(Option<$raw_type>);

        impl $struct_name {
            pub fn new(val: Option<$raw_type>) -> Self {
                Self(val)
            }

            pub fn set(&mut self, val: Option<$raw_type>) {
                self.0 = val;
            }

            pub fn set_unchecked(&mut self, val: $raw_type) {
                self.0 = Some(val);
            }

            pub const fn get(&self) -> &Option<$raw_type> {
                &self.0
            }

            pub const fn as_ref(&self) -> Option<&$raw_type> {
                self.0.as_ref()
            }

            pub fn get_mut(&mut self) -> &mut Option<$raw_type> {
                &mut self.0
            }

            pub fn as_mut(&mut self) -> Option<&mut $raw_type> {
                self.0.as_mut()
            }

            pub fn into_inner(self) -> Option<$raw_type> {
                self.0
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            trivial_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(trivial_impl);

// implement Display trait for scalar types
macro_rules! from_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<$raw_type> for $struct_name {
            fn from(value: $raw_type) -> Self {
                Self::new(Some(value))
            }
        }

        impl TryFrom<$struct_name> for $raw_type {
            type Error = RuntimeError;

            fn try_from(value: $struct_name) -> Result<Self, Self::Error> {
                value.0.ok_or(RuntimeError::ConvertFail).map(|v| v)
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            from_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(from_impl);

// implement Display trait for scalar types
macro_rules! from_impl2 {
    (DolphinString, DolphinString) => {
        from_impl2!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<$struct_name> for ScalarKind {
            fn from(value: $struct_name) -> Self {
                Self::$enum_name(value)
            }
        }

        impl TryFrom<ScalarKind> for $struct_name {
            type Error = RuntimeError;

            fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
                match value {
                    ScalarKind::$enum_name(value) => Ok(value),
                    _ => Err(RuntimeError::ConvertFail),
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            from_impl2!($struct_name, $struct_name);
        )*
    };
}
for_all_scalars!(from_impl2);

// implement Display trait for scalar types
macro_rules! display_impl {
    ($struct_name:ident) => {
        impl Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if let Some(val) = self.as_ref() {
                    write!(f, "{}", val)
                } else {
                    write!(f, "")
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
for_all_scalars!(display_impl);

// implement NotDecimal trait for scalar types
pub trait NotDecimal {}

impl NotDecimal for () {}

macro_rules! non_decimal_marker {
    (Decimal32 | Decimal64 | Decimal128) => {};

    ($struct_name:ident) => {
        impl NotDecimal for $struct_name {}
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            non_decimal_marker!($struct_name);
        )*
    };
}
for_all_scalars!(non_decimal_marker);

// implement Scalar trait for scalar types
macro_rules! scalar_trait_impl {
    (String, DolphinString) => {
        impl Scalar for DolphinString {
            type RawType = String;
            type RefType<'a> = &'a str;

            fn new(raw: Self::RawType) -> Self {
                Self::new(Some(raw))
            }

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data.to_string()
            }

            fn data_type() -> DataType {
                DataType::DolphinString
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        impl Scalar for $struct_name {
            type RawType = $raw_type;
            type RefType<'a> = $raw_type;

            fn new(raw: Self::RawType) -> Self {
                Self::new(Some(raw.into()))
            }

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data
            }

            fn data_type() -> DataType {
                DataType::$struct_name
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            scalar_trait_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(scalar_trait_impl);
