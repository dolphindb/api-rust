mod basic;
mod constant;
mod dictionary;
mod pair;
mod scalar;
mod set;
mod vector;
mod vector_kind;

pub use basic::{Basic, DataCategory, DataForm, DataType};
pub use constant::{Constant, ConstantKind};
pub use dictionary::Dictionary;
pub use pair::Pair;
pub use scalar::{Scalar, ScalarKind};
pub use set::Set;
pub use vector::Vector;
pub use vector_kind::VectorKind;

use chrono::{Datelike, Duration, NaiveDate};
use ordered_float::OrderedFloat;
use std::fmt::{self, Debug, Display};

use crate::error::RuntimeError;

// helper iterator macro
macro_rules! for_all_scalars {
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
            (i32, Date),
            (i32, Month),
            (i32, Time),
            (i32, Minute),
            (i32, Second),
            (i32, DateTime),
            (i32, DateHour),
            (i64, TimeStamp),
            (i64, NanoTime),
            (i64, NanoTimeStamp)
        );
    };
}

// implement trivial functions
macro_rules! trivial_impl {
    (i8, Bool) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)] // TODO ??
        pub struct Bool(i8);

        impl Bool {
            pub fn new(val: i8) ->Self {
                Self(val)
            }

            pub fn get_raw(&self) -> i8 {
                if self.0 != 0 && self.0 != i8::MIN {
                    1
                } else {
                    self.0
                }
            }

            pub fn set_raw(&mut self, val: i8) {
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

            pub fn get_raw(&self) -> f32 {
                self.0.0
            }

            pub fn set_raw(&mut self, val: f32) {
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

            pub fn get_raw(&self) -> f64 {
                self.0.0
            }

            pub fn set_raw(&mut self, val: f64) {
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

            pub fn get_raw(&self) -> &str {
                &self.0
            }

            pub fn set_raw(&mut self, val: String) {
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

            pub fn set_raw(&mut self, val: $raw_type) {
                self.0 = val;
            }

            pub fn get_raw(&self) -> $raw_type {
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

// implement null value and get/set functions
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

    pub fn get_bool(&self) -> Option<bool> {
        if self.0 == i8::MIN {
            None
        } else if self.0 == 0 {
            Some(false)
        } else {
            Some(true)
        }
    }

    pub fn set_bool(&mut self, val: Option<bool>) {
        if let Some(v) = val {
            self.0 = if v { 1 } else { 0 };
        } else {
            self.0 = i8::MIN;
        }
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

    pub fn get_char(&self) -> Option<i8> {
        let val = self.get_raw();
        if val == i8::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_char(&mut self, val: Option<i8>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i8::MIN;
        }
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

    pub fn get_short(&self) -> Option<i16> {
        let val = self.get_raw();
        if val == i16::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_short(&mut self, val: Option<i16>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i16::MIN;
        }
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

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
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

    pub fn get_long(&self) -> Option<i64> {
        let val = self.get_raw();
        if val == i64::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_long(&mut self, val: Option<i64>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i64::MIN;
        }
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

    pub fn get_float(&self) -> Option<f32> {
        let val = self.get_raw();
        if val == f32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_float(&mut self, val: Option<f32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = OrderedFloat::<f32>::from(f32::MIN);
        }
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

    pub fn get_double(&self) -> Option<f64> {
        let val = self.get_raw();
        if val == f64::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_double(&mut self, val: Option<f64>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = OrderedFloat::<f64>::from(f64::MIN);
        }
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

    pub fn get_string(&self) -> Option<&str> {
        let val = self.get_raw();
        if val.is_empty() {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_string(&mut self, val: Option<String>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = String::new();
        }
    }
}

impl Date {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
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

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
    }
}

impl Time {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
    }
}

impl Minute {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
    }
}

impl Second {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
    }
}

impl DateTime {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
    }
}

impl DateHour {
    pub fn new_null() -> Self {
        Self(i32::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i32::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i32::MIN
    }

    pub fn get_int(&self) -> Option<i32> {
        let val = self.get_raw();
        if val == i32::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_int(&mut self, val: Option<i32>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i32::MIN;
        }
    }
}

impl TimeStamp {
    pub fn new_null() -> Self {
        Self(i64::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i64::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i64::MIN
    }

    pub fn get_long(&self) -> Option<i64> {
        let val = self.get_raw();
        if val == i64::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_long(&mut self, val: Option<i64>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i64::MIN;
        }
    }
}

impl NanoTime {
    pub fn new_null() -> Self {
        Self(i64::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i64::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i64::MIN
    }

    pub fn get_long(&self) -> Option<i64> {
        let val = self.get_raw();
        if val == i64::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_long(&mut self, val: Option<i64>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i64::MIN;
        }
    }
}

impl NanoTimeStamp {
    pub fn new_null() -> Self {
        Self(i64::MIN)
    }

    pub fn set_null(&mut self) {
        self.0 = i64::MIN;
    }

    pub fn is_null(&self) -> bool {
        self.0 == i64::MIN
    }

    pub fn get_long(&self) -> Option<i64> {
        let val = self.get_raw();
        if val == i64::MIN {
            None
        } else {
            Some(val)
        }
    }

    pub fn set_long(&mut self, val: Option<i64>) {
        if let Some(v) = val {
            self.set_raw(v);
        } else {
            self.0 = i64::MIN;
        }
    }
}

// implement From ant TryFrom trarits
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

macro_rules! from_impl {
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
            from_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(from_impl);

// implement Display trait
macro_rules! display_impl {
    (i8, bool) => {
        impl Display for Bool {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if Some(v) = self.get_bool() {
                    write!(f, "{}", v)
                } else {
                    write!(f, "NULL")
                }
            }
        }
    };

    (i32, Date) => {
        // e.g. 2013.06.13
        impl Display for Date {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64);
                write!(f, "{:04}.{:02}.{02}", date.year(), date.month(), date.day())
            }
        }
    };

    (i32, Month) => {
        // e.g. 2012.06M
        impl Display for Month {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                write!(f, "{:04}.{:02}M", self.0 / 12, self.0 % 12 + 1)
            }
        }
    };

    (i32, Time) => {
        // e.g. 13:30:10.008
        impl Display for Time {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                write!(f, "{:02}:{:02}:{:02}.{:03}", self.0 / 3600000, self.0 % 3600000 / 60000, self.0 % 60000 / 1000,
                       self.0 % 1000)
            }
        }
    };

    (i32, Minute) => {
        // e.g. 13:30m
        impl Display for Minute {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                write!(f, "{:02}:{:02}m", self.0 / 60, self.0 % 60)
            }
        }
    };

    (i32, Second) => {
        // e.g. 13:30:10
        impl Display for Second {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                write!(f, "{:02}:{:02}:{:02}", self.0 / 3600, self.0 % 3600 / 60, self.0 % 60)
            }
        }
    };

    (i32, DateTime) => {
        // e.g. 2012.06.13 13:30:10
        impl Display for DateTime {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 86400);
                write!(f, "{:04}.{:02}.{:02} {:02}:{:02}:{:02}", date.year(), date.month(), date.day(),
                       self.0 % 86400 / 3600, self.0 % 3600 / 60, self.0 % 60)
            }
        }
    };

    (i32, DateHour) => {
        // e.g. 2012.06.13T13
        impl Display for DateHour {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0  as i64 / 24);
                write!(f, "{:04}.{:02}.{:02}T{:02}", date.year(), date.month(), date.day(), self.0 % 24)
            }
        }
    };

    (i64, TimeStamp) => {
        // e.g. 2012.06.13 13:30:10.008
        impl Display for TimeStamp {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000);
                write!(f, "{:04}.{:02}.{:02} {:02}:{:02}:{:02}:{:03}", date.year(), date.month(), date.day(),
                       self.0 % 86400000 / 3600000, self.0 % 3600000 / 60000, self.0 % 60000 / 1000, self.0 % 1000)
            }
        }
    };

    (i64, NanoTime) => {
        // e.g. 13:30:10.008007006
        impl Display for NanoTime {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                write!(f, "{:02}:{:02}:{:02}.{:09}", self.0 / 3600000000000, self.0 % 3600000000000 / 60000000000,
                       self.0 % 60000000000 / 1000000000, self.0 % 1000000000)
            }
        }
    };

    (i64, NanoTimeStamp) => {
        // e.g. 2012.06.13 13:30:10.008007006
        impl Display for NanoTimeStamp {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")?;
                }

                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000000000);
                write!(f, "{:04}.{:02}.{:02} {:02}:{:02}:{:02}:{:09}", date.year(), date.month(), date.day(),
                       self.0 % 86400000000000 / 3600000000000, self.0 % 3600000000000 / 60000000000,
                       self.0 % 60000000000 / 1000000000, self.0 % 1000000000)
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        impl Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.is_null() {
                    write!(f, "NULL")
                } else {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            display_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(display_impl);

// implement ConcreteScalar trait
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

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType { // TODO ??
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
            concrete_scalar_trait_impl!($raw_type, $struct_name);
        )*
    };
}
for_all_scalars!(concrete_scalar_trait_impl);

// implement NotDecimal trait
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
