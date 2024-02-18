//! Scalar type wrappers to make our lives easier when interact with DolphinDB type system.

mod constant;
mod dictionary;
mod pair;
mod scalar;
mod set;
mod vector;

use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
pub use constant::*;
use ordered_float::OrderedFloat;
pub use pair::*;
use rust_decimal::Decimal;
pub use scalar::*;
use std::fmt::{self, Debug, Display};
pub use vector::*;

type OrderedFloatF32 = OrderedFloat<f32>;
type OrderedFloatF64 = OrderedFloat<f64>;

macro_rules! trivial_impl {
    ($raw_type:tt, $struct_name:ident, $data_type:tt) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct $struct_name(Option<$raw_type>);

        impl $struct_name {
            pub const DATA_BYTE: u8 = $data_type;

            pub fn new(val: Option<$raw_type>) -> Self {
                Self(val)
            }

            pub const fn data_type(&self) -> u8 {
                Self::DATA_BYTE
            }

            pub fn set(&mut self, val: Option<$raw_type>) {
                self.0 = val;
            }

            pub fn set_unchecked(&mut self, val: $raw_type) {
                self.0 = Some(val);
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $data_type:tt)), *) => {
        $(
            trivial_impl!($raw_type, $struct_name, $data_type);
        )*
    };
}

trivial_impl!(
    (bool, Bool, 1),
    (u8, Char, 2),
    (i16, Short, 3),
    (i32, Int, 4),
    (i64, Long, 5),
    (NaiveDate, Date, 6),
    (NaiveDate, Month, 7),
    (NaiveTime, Time, 8),
    (NaiveTime, Minute, 9),
    (NaiveTime, Second, 10),
    (NaiveDateTime, DateTime, 11),
    (NaiveDateTime, TimeStamp, 12),
    (NaiveTime, NanoTime, 13),
    (NaiveDateTime, NanoTimeStamp, 14),
    (OrderedFloatF32, Float, 15),
    (OrderedFloatF64, Double, 16),
    (NaiveDateTime, DateHour, 28),
    (Decimal, Decimal32, 37),
    (Decimal, Decimal64, 38),
    (Decimal, Decimal128, 39)
);

#[derive(Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct DolphinString(Option<String>);

impl DolphinString {
    pub const DATA_BYTE: u8 = 18;

    pub fn new(val: Option<String>) -> Self {
        Self(val)
    }

    pub fn data_type(&self) -> u8 {
        Self::DATA_BYTE
    }

    pub fn set(&mut self, val: Option<String>) {
        self.0 = val;
    }

    pub fn set_unchecked(&mut self, val: String) {
        self.0 = Some(val)
    }
}

macro_rules! getter_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl $struct_name {
            pub const fn is_null(&self) -> bool {
                self.0.is_none()
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
            getter_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! from_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<$raw_type> for $struct_name {
            fn from(value: $raw_type) -> Self {
                Self::new(Some(value))
            }
        }

        impl TryFrom<$struct_name> for $raw_type {
            type Error = ();

            fn try_from(value: $struct_name) -> Result<Self, Self::Error> {
                value.0.ok_or(()).map(|v| v)
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            from_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! try_from_impl {
    (DolphinString, DolphinString) => {
        try_from_impl!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<$struct_name> for ScalarImpl {
            fn from(value: $struct_name) -> Self {
                Self::$enum_name(value)
            }
        }

        impl TryFrom<ScalarImpl> for $struct_name {
            type Error = ();

            fn try_from(value: ScalarImpl) -> Result<Self, Self::Error> {
                match value {
                    ScalarImpl::$enum_name(value) => Ok(value),
                    _ => Err(()),
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            try_from_impl!($struct_name, $struct_name);
        )*
    };
}

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

            fn data_type() -> u8 {
                Self::DATA_BYTE
            }

            fn is_null(&self) -> bool {
                self.0.is_some()
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

            fn data_type() -> u8 {
                Self::DATA_BYTE
            }

            fn is_null(&self) -> bool {
                self.0.is_some()
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

pub trait IsDecimal {}

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
            (Decimal, Decimal32),
            (Decimal, Decimal64),
            (Decimal, Decimal128),
            (u8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (OrderedFloatF32, Float),
            (OrderedFloatF64, Double)
        );
    };
}

pub(crate) use for_all_scalars;

for_all_scalars!(getter_impl);

for_all_scalars!(from_impl);

for_all_scalars!(try_from_impl);

for_all_scalars!(scalar_trait_impl);

for_all_scalars!(display_impl);

for_all_scalars!(non_decimal_marker);

impl NotDecimal for () {}
