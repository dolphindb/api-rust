mod basic;
mod constant;
mod dictionary;
mod pair;
mod scalar;
mod set;
mod vector;

pub use basic::{Basic, DataCategory, DataForm, DataType};
pub use constant::{Constant, ConstantKind};
pub use dictionary::Dictionary;
pub use pair::Pair;
pub use scalar::{Scalar, ScalarKind};
pub use set::Set;
pub use vector::VectorKind;

use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use ordered_float::OrderedFloat;
use std::fmt::{self, Debug, Display};

use crate::error::RuntimeError;

// helper iterator macro
macro_rules! for_all_scalars {
    ($macro:tt) => {
        $macro!(
            (NaiveDate, Date),
            (NaiveTime, Time),
            (NaiveTime, Minute),
            (NaiveTime, Second),
            (NaiveDateTime, DateTime),
            (NaiveDateTime, TimeStamp),
            (NaiveTime, NanoTime),
            (NaiveDateTime, NanoTimeStamp),
            (NaiveDateTime, DateHour)
        );
    };
}

//  implement trivial functions
macro_rules! trivial_impl {
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

// implement From trait for scalar types
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

// implement From trait for scalar types
macro_rules! from_impl2 {
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

// implement ConcreteScalar trait for every scalar types
pub trait ConcreteScalar: Basic {
    type RawType: Send + Sync + Clone;
    type RefType<'a>: Send + Copy;

    fn new(raw: Self::RawType) -> Self;
    fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType;
    fn data_type() -> DataType;
    fn is_null(&self) -> bool;
}

impl ConcreteScalar for () {
    type RawType = ();
    type RefType<'a> = ();

    fn new(_: Self::RawType) -> Self {}
    fn to_owned(_: Self::RefType<'_>) -> Self::RawType {}

    fn data_type() -> DataType {
        DataType::Void
    }

    fn is_null(&self) -> bool {
        true
    }
}

macro_rules! concrete_scalar_trait_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl ConcreteScalar for $struct_name {
            type RawType = $raw_type;
            type RefType<'a> = $raw_type;

            fn new(raw: Self::RawType) -> Self {
                Self::new(Some(raw.into()))
            }

            // TODO ??
            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data
            }

            fn data_type() -> DataType {
                DataType::$struct_name
            }

            fn is_null(&self) -> bool {
                self.0.is_some()
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            concrete_scalar_trait_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(concrete_scalar_trait_impl);

// ---------------------------------------------------------------------------------- rewrite

// helper iterator macro
macro_rules! for_all_scalars2 {
    ($macro:tt) => {
        $macro!(
            (i8, Bool),
            (i8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (f32, Float),
            (f64, Double),
            (String, DolphinString),
            (i32, Month)
        );
    };
}

// implement trivial functions
macro_rules! trivial_impl2 {
    (i8, Bool) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)] // TODO ??
        pub struct Bool(i8);

        impl Bool {
            pub fn new(val: i8) ->Self {
                Self(val)
            }

            pub fn get(&self) -> i8 {
                if self.0 != 0 && self.0 != i8::MIN {
                    1
                } else {
                    self.0
                }
            }

            pub fn set(&mut self, val: i8) {
                self.0 = val;
            }
        }
    };

    (f32, Float) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct Float(OrderedFloat<f32>);

        impl Float {
            pub fn new(val: f32) ->Self {
                Self(OrderedFloat::<f32>::from(val))
            }

            pub fn get(&self) -> f32 {
                self.0.0
            }

            pub fn set(&mut self, val: f32) {
                self.0.0 = val;
            }
        }
    };

    (f64, Double) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct Double(OrderedFloat<f64>);

        impl Double {
            pub fn new(val: f64) ->Self {
                Self(OrderedFloat::<f64>::from(val))
            }

            pub fn get(&self) -> f64 {
                self.0.0
            }

            pub fn set(&mut self, val: f64) {
                self.0.0 = val;
            }
        }
    };

    (String, DolphinString) => {
        #[derive(Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct DolphinString(String);

        impl DolphinString{
            pub fn new(val: String) -> Self {
                Self(val)
            }

            pub fn get(&self) -> &str {
                &self.0
            }

            pub fn set(&mut self, val: String) {
                self.0 = val;
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)] // TODO ??
        pub struct $struct_name($raw_type);

        impl $struct_name {
            pub fn new(val: $raw_type) -> Self {
                Self(val)
            }

            pub fn set(&mut self, val: $raw_type) {
                self.0 = val;
            }

            pub fn get(&self) -> $raw_type {
                self.0
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            trivial_impl2!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars2!(trivial_impl2);

impl Bool {
    pub fn new_null() -> Self {
        Self(i8::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i8::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i8::MIN
    }
}

impl Char {
    pub fn new_null() -> Self {
        Self(i8::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i8::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i8::MIN
    }
}

impl Short {
    pub fn new_null() -> Self {
        Self(i16::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i16::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i16::MIN
    }
}

impl Int {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }
}

impl Long {
    pub fn new_null() -> Self {
        Self(i64::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i64::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i64::MIN
    }
}

impl Float {
    pub fn new_null() -> Self {
        Self(OrderedFloat::<f32>::from(f32::MIN))
    }

    pub fn set_null(&mut self) {
        self.0 .0 = f32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 .0 == f32::MIN
    }
}

impl Double {
    pub fn new_null() -> Self {
        Self(OrderedFloat::<f64>::from(f64::MIN))
    }

    pub fn set_null(&mut self) {
        self.0 .0 = f64::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 .0 == f64::MIN
    }
}

impl DolphinString {
    pub fn new_null() -> Self {
        Self(String::new())
    }

    pub fn set_null(&mut self) {
        self.0 = String::new();
    }

    pub fn is_null(&self) -> bool {
        self.0.is_empty()
    }
}

impl Month {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }
}

// implement From ant TryFrom trarits
macro_rules! from_impl3 {
    (i32, Month) => {
        impl From<i32> for Month {
            fn from(value: i32) -> Self {
                Self::new(value)
            }
        }

        impl From<Month> for i32 {
            fn from(value: Month) -> Self {
                value.0
            }
        }

        impl From<Month> for ScalarKind {
            fn from(value: Month) -> Self {
                Self::Month(value)
            }
        }

        impl TryFrom<ScalarKind> for Month {
            type Error = RuntimeError;

            fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
                match value {
                    ScalarKind::Month(value) => Ok(value),
                    _ => Err(RuntimeError::ConvertFail),
                }
            }
        }
    };

    (f32, Float) => {
        impl From<f32> for Float {
            fn from(value: f32) -> Self {
                Self::new(value)
            }
        }

        impl From<Float> for f32 {
            fn from(value: Float) -> Self {
                value.0.0
            }
        }

        impl From<Float> for ScalarKind {
            fn from(value: Float) -> Self {
                Self::Float(value)
            }
        }

        impl TryFrom<ScalarKind> for Float {
            type Error = RuntimeError;

            fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
                match value {
                    ScalarKind::Float(value) => Ok(value),
                    _ => Err(RuntimeError::ConvertFail),
                }
            }
        }
    };

    (f64, Double) => {
        impl From<f64> for Double {
            fn from(value: f64) -> Self {
                Self::new(value)
            }
        }

        impl From<Double> for f64 {
            fn from(value: Double) -> Self {
                value.0.0
            }
        }

        impl From<Double> for ScalarKind {
            fn from(value: Double) -> Self {
                Self::Double(value)
            }
        }

        impl TryFrom<ScalarKind> for Double {
            type Error = RuntimeError;

            fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
                match value {
                    ScalarKind::Double(value) => Ok(value),
                    _ => Err(RuntimeError::ConvertFail),
                }
            }
        }
    };

    (String, DolphinString) => {
        impl From<String> for DolphinString {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl From<DolphinString> for String {
            fn from(value: DolphinString) -> Self {
                value.0
            }
        }

        impl From<DolphinString> for ScalarKind {
            fn from(value: DolphinString) -> Self {
                Self::String(value)
            }
        }

        impl TryFrom<ScalarKind> for DolphinString {
            type Error = RuntimeError;

            fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
                match value {
                    ScalarKind::String(value) => Ok(value),
                    _ => Err(RuntimeError::ConvertFail),
                }
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        impl From<$raw_type> for $struct_name {
            fn from(value: $raw_type) -> Self {
                Self::new(value)
            }
        }

        impl From<$struct_name> for $raw_type {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }

        impl From<$struct_name> for ScalarKind {
            fn from(value: $struct_name) -> Self {
                Self::$struct_name(value)
            }
        }

        impl TryFrom<ScalarKind> for $struct_name {
            type Error = RuntimeError;

            fn try_from(value: ScalarKind) -> Result<Self, Self::Error> {
                match value {
                    ScalarKind::$struct_name(value) => Ok(value),
                    _ => Err(RuntimeError::ConvertFail),
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            from_impl3!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars2!(from_impl3);

// implement Display trait
macro_rules! display_impl2 {
    (i32, Month) => {
        impl Display for Month {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:04}.{:02}M", self.0 / 12, (self.0 % 12) + 1)
            }
        }
    };

    ($struct_name:ident) => {
        impl Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            display_impl2!($struct_name);
        )*
    };
}
for_all_scalars2!(display_impl2);

// implement ConcreteScalar trait
macro_rules! concrete_scalar_trait_impl2 {
    (String, DolphinString) => {
        impl ConcreteScalar for DolphinString {
            type RawType = String;
            type RefType<'a> = &'a str;

            fn new(raw: Self::RawType) -> Self {
                Self::new(raw)
            }

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data.to_string()
            }

            fn data_type() -> DataType {
                DataType::DolphinString
            }

            fn is_null(&self) -> bool {
                self.0.is_empty()
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        impl ConcreteScalar for $struct_name {
            type RawType = $raw_type;
            type RefType<'a> = $raw_type;

            fn new(raw: Self::RawType) -> Self {
                Self::new(raw)
            }

            // TODO ??
            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data
            }

            fn data_type() -> DataType {
                DataType::$struct_name
            }

            fn is_null(&self) -> bool {
                $struct_name::is_null(self)
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            concrete_scalar_trait_impl2!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars2!(concrete_scalar_trait_impl2);

// implement NotDecimal trait
macro_rules! non_decimal_marker2 {
    (Decimal32 | Decimal64 | Decimal128) => {};

    ($struct_name:ident) => {
        impl NotDecimal for $struct_name {}
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            non_decimal_marker2!($struct_name);
        )*
    };
}
for_all_scalars2!(non_decimal_marker2);
