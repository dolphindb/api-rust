use super::{
    Bool, Char, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long, Minute, Month,
    NanoTime, NanoTimeStamp, ScalarKind, Second, Short, Time, TimeStamp,
};
use crate::error::RuntimeError;

// data type enum implementation
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
    DolphinString = 18,
    Any = 25,
    DateHour = 28,
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
            18 => Some(DataType::DolphinString),
            25 => Some(DataType::Any),
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
            DataType::DolphinString => 18,
            DataType::Any => 25,
            DataType::DateHour => 28,
        }
    }
}

// data form
pub enum DataForm {
    Scalar,
    Vector,
    Pair,
    Placeholder,
    Set,
    Dictionary,
    Table,
}

impl DataForm {
    pub fn to_u8(&self) -> u8 {
        match self {
            DataForm::Scalar => 0,
            DataForm::Vector => 1,
            DataForm::Pair => 2,
            DataForm::Placeholder => 3,
            DataForm::Set => 4,
            DataForm::Dictionary => 5,
            DataForm::Table => 6,
        }
    }
}

// data category
pub enum DataCategory {
    Nothing,
    Logical,
    Integral,
    Floating,
    Temporal,
    Literal,
    Mixed = 7,
}

// todo: use From or TryFrom trait
impl DataCategory {
    pub fn from_data_type(data_type: &DataType) -> DataCategory {
        match data_type {
            DataType::Void => DataCategory::Nothing,
            DataType::Bool => DataCategory::Logical,
            DataType::Char | DataType::Short | DataType::Int | DataType::Long => {
                DataCategory::Integral
            }
            DataType::Float | DataType::Double => DataCategory::Floating,
            DataType::Date
            | DataType::Month
            | DataType::Time
            | DataType::Minute
            | DataType::Second
            | DataType::DateTime
            | DataType::TimeStamp
            | DataType::NanoTime
            | DataType::NanoTimeStamp
            | DataType::DateHour => DataCategory::Temporal,
            DataType::DolphinString => DataCategory::Literal,
            DataType::Any => DataCategory::Mixed,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            DataCategory::Nothing => 0,
            DataCategory::Logical => 1,
            DataCategory::Integral => 2,
            DataCategory::Floating => 3,
            DataCategory::Temporal => 4,
            DataCategory::Literal => 5,
            DataCategory::Mixed => 7,
        }
    }
}

pub trait Basic: Send + Sync + Clone {
    fn data_type(&self) -> DataType;
    fn data_category(&self) -> DataCategory;

    fn data_form(&self) -> DataForm {
        // the default implementation of all Scalar and ScalarKind
        DataForm::Scalar
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        // the default implementation of the types other than Scalar, ScalarKind and ConstantKind
        Err(RuntimeError::NotSupportInterface)
    }

    fn size(&self) -> usize {
        // the default implementation of all Scalar and ScalarKind
        1
    }

    fn is_empty(&self) -> bool {
        self.size() == 0
    }

    // default implementation of Basic getters
    fn get_bool(&self) -> Result<bool, RuntimeError> {
        Err(RuntimeError::NotBoolScalar)
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        Err(RuntimeError::NotCharScalar)
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        Err(RuntimeError::NotShortScalar)
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        Err(RuntimeError::NotIntScalar)
    }
    fn get_long(&self) -> Result<i64, RuntimeError> {
        Err(RuntimeError::NotLongScalar)
    }
    fn get_float(&self) -> Result<f32, RuntimeError> {
        Err(RuntimeError::NotFloatScalar)
    }
    fn get_double(&self) -> Result<f64, RuntimeError> {
        Err(RuntimeError::NotDoubleScalar)
    }
    fn get_string(&self) -> Result<&str, RuntimeError> {
        Err(RuntimeError::NotStringScalar)
    }
}

// implement Basic trait for ScalarKind
impl Basic for ScalarKind {
    fn data_type(&self) -> DataType {
        match self {
            ScalarKind::Void => DataType::Void,
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

    fn data_category(&self) -> DataCategory {
        match self {
            ScalarKind::Void => DataCategory::Nothing,
            ScalarKind::Bool(obj) => obj.data_category(),
            ScalarKind::Char(obj) => obj.data_category(),
            ScalarKind::Short(obj) => obj.data_category(),
            ScalarKind::Int(obj) => obj.data_category(),
            ScalarKind::Long(obj) => obj.data_category(),
            ScalarKind::Date(obj) => obj.data_category(),
            ScalarKind::Month(obj) => obj.data_category(),
            ScalarKind::Time(obj) => obj.data_category(),
            ScalarKind::Minute(obj) => obj.data_category(),
            ScalarKind::Second(obj) => obj.data_category(),
            ScalarKind::DateTime(obj) => obj.data_category(),
            ScalarKind::TimeStamp(obj) => obj.data_category(),
            ScalarKind::NanoTime(obj) => obj.data_category(),
            ScalarKind::NanoTimeStamp(obj) => obj.data_category(),
            ScalarKind::Float(obj) => obj.data_category(),
            ScalarKind::Double(obj) => obj.data_category(),
            ScalarKind::String(obj) => obj.data_category(),
            ScalarKind::DateHour(obj) => obj.data_category(),
        }
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        match self {
            ScalarKind::Void => Ok(true),
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
            _ => Err(RuntimeError::NotBoolScalar),
        }
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        match self {
            ScalarKind::Char(obj) => obj.get_char(),
            _ => Err(RuntimeError::NotCharScalar),
        }
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        match self {
            ScalarKind::Short(obj) => obj.get_short(),
            _ => Err(RuntimeError::NotShortScalar),
        }
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        match self {
            ScalarKind::Int(obj) => obj.get_int(),
            _ => Err(RuntimeError::NotIntScalar),
        }
    }
    fn get_long(&self) -> Result<i64, RuntimeError> {
        match self {
            ScalarKind::Long(obj) => obj.get_long(),
            _ => Err(RuntimeError::NotLongScalar),
        }
    }
    fn get_float(&self) -> Result<f32, RuntimeError> {
        match self {
            ScalarKind::Float(obj) => obj.get_float(),
            _ => Err(RuntimeError::NotFloatScalar),
        }
    }
    fn get_double(&self) -> Result<f64, RuntimeError> {
        match self {
            ScalarKind::Double(obj) => obj.get_double(),
            _ => Err(RuntimeError::NotDoubleScalar),
        }
    }
    fn get_string(&self) -> Result<&str, RuntimeError> {
        match self {
            ScalarKind::String(obj) => obj.get_string(),
            _ => Err(RuntimeError::NotStringScalar),
        }
    }
}

// implement Basic trait for scalar types
impl Basic for () {
    fn data_type(&self) -> DataType {
        DataType::Void
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Nothing
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(true)
    }
}

impl Basic for Bool {
    fn data_type(&self) -> DataType {
        DataType::Bool
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Logical
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_bool(&self) -> Result<bool, RuntimeError> {
        // TODO bool 的 null 值是什么？？
        self.0.map_or(Ok(false), Ok) // todo: bug
    }
}

impl Basic for Date {
    fn data_type(&self) -> DataType {
        DataType::Date
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for Month {
    fn data_type(&self) -> DataType {
        DataType::Month
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for Time {
    fn data_type(&self) -> DataType {
        DataType::Time
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for Minute {
    fn data_type(&self) -> DataType {
        DataType::Minute
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for Second {
    fn data_type(&self) -> DataType {
        DataType::Second
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for DateTime {
    fn data_type(&self) -> DataType {
        DataType::DateTime
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for TimeStamp {
    fn data_type(&self) -> DataType {
        DataType::TimeStamp
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for NanoTime {
    fn data_type(&self) -> DataType {
        DataType::NanoTime
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for NanoTimeStamp {
    fn data_type(&self) -> DataType {
        DataType::NanoTimeStamp
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for DolphinString {
    fn data_type(&self) -> DataType {
        DataType::DolphinString
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Literal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_string(&self) -> Result<&str, RuntimeError> {
        Ok(self.0.as_deref().unwrap_or(""))
    }
}

impl Basic for DateHour {
    fn data_type(&self) -> DataType {
        DataType::DateHour
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Temporal
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }
}

impl Basic for Char {
    fn data_type(&self) -> DataType {
        DataType::Char
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Integral
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_char(&self) -> Result<u8, RuntimeError> {
        self.0.map_or(Ok(u8::MIN), Ok)
    }
}

impl Basic for Short {
    fn data_type(&self) -> DataType {
        DataType::Short
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Integral
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_short(&self) -> Result<i16, RuntimeError> {
        self.0.map_or(Ok(i16::MIN), Ok)
    }
}

impl Basic for Int {
    fn data_type(&self) -> DataType {
        DataType::Int
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Integral
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_int(&self) -> Result<i32, RuntimeError> {
        self.0.map_or(Ok(i32::MIN), Ok)
    }
}

impl Basic for Long {
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Integral
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_long(&self) -> Result<i64, RuntimeError> {
        self.0.map_or(Ok(i64::MIN), Ok)
    }
}

impl Basic for Float {
    fn data_type(&self) -> DataType {
        DataType::Float
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Floating
    }

    fn get_float(&self) -> Result<f32, RuntimeError> {
        Ok(self
            .0
            .map_or(f32::MIN, |ordered_float| ordered_float.into_inner()))
    }
}

impl Basic for Double {
    fn data_type(&self) -> DataType {
        DataType::Double
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::Floating
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.is_some())
    }

    fn get_double(&self) -> Result<f64, RuntimeError> {
        Ok(self
            .0
            .map_or(f64::MIN, |ordered_float| ordered_float.into_inner()))
    }
}
