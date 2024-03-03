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
    Placeholder1,
    DolphinString,
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
// todo

pub trait Basic: Send + Sync + Clone {
    fn data_type(&self) -> DataType;

    fn data_form(&self) -> DataForm {
        // the default implementation of all scalar types and ScalarKind
        DataForm::Scalar
    }

    fn is_null(&self) -> bool {
        false
    }

    // default implementation of Basic getters
    fn get_bool(&self) -> Result<bool, RuntimeError> {
        Err(RuntimeError::GetBoolFail)
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        Err(RuntimeError::GetCharFail)
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        Err(RuntimeError::GetShortFail)
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        Err(RuntimeError::GetIntFail)
    }
    fn get_long(&self) -> Result<i64, RuntimeError> {
        Err(RuntimeError::GetLongFail)
    }
    fn get_float(&self) -> Result<f32, RuntimeError> {
        Err(RuntimeError::GetFloatFail)
    }
    fn get_double(&self) -> Result<f64, RuntimeError> {
        Err(RuntimeError::GetDoubleFail)
    }
    fn get_string(&self) -> Result<&str, RuntimeError> {
        Err(RuntimeError::GetStringFail)
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

    // implementation of Basic getters
    fn get_bool(&self) -> Result<bool, RuntimeError> {
        match self {
            ScalarKind::Bool(obj) => obj.get_bool(),
            _ => Err(RuntimeError::GetBoolFail),
        }
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        match self {
            ScalarKind::Char(obj) => obj.get_char(),
            _ => Err(RuntimeError::GetCharFail),
        }
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        match self {
            ScalarKind::Short(obj) => obj.get_short(),
            _ => Err(RuntimeError::GetShortFail),
        }
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        match self {
            ScalarKind::Int(obj) => obj.get_int(),
            _ => Err(RuntimeError::GetIntFail),
        }
    }
    fn get_long(&self) -> Result<i64, RuntimeError> {
        match self {
            ScalarKind::Long(obj) => obj.get_long(),
            _ => Err(RuntimeError::GetLongFail),
        }
    }
    fn get_float(&self) -> Result<f32, RuntimeError> {
        match self {
            ScalarKind::Float(obj) => obj.get_float(),
            _ => Err(RuntimeError::GetFloatFail),
        }
    }
    fn get_double(&self) -> Result<f64, RuntimeError> {
        match self {
            ScalarKind::Double(obj) => obj.get_double(),
            _ => Err(RuntimeError::GetDoubleFail),
        }
    }
    fn get_string(&self) -> Result<&str, RuntimeError> {
        match self {
            ScalarKind::String(obj) => obj.get_string(),
            _ => Err(RuntimeError::GetStringFail),
        }
    }
}

// implement Basic trait for scalar types
impl Basic for () {
    fn data_type(&self) -> DataType {
        DataType::Void
    }

    fn is_null(&self) -> bool {
        true
    }
}

impl Basic for Bool {
    fn data_type(&self) -> DataType {
        DataType::Bool
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
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

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for Month {
    fn data_type(&self) -> DataType {
        DataType::Month
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for Time {
    fn data_type(&self) -> DataType {
        DataType::Time
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for Minute {
    fn data_type(&self) -> DataType {
        DataType::Minute
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for Second {
    fn data_type(&self) -> DataType {
        DataType::Second
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for DateTime {
    fn data_type(&self) -> DataType {
        DataType::DateTime
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for TimeStamp {
    fn data_type(&self) -> DataType {
        DataType::TimeStamp
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for NanoTime {
    fn data_type(&self) -> DataType {
        DataType::NanoTime
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for NanoTimeStamp {
    fn data_type(&self) -> DataType {
        DataType::NanoTimeStamp
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for DolphinString {
    fn data_type(&self) -> DataType {
        DataType::DolphinString
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }

    fn get_string(&self) -> Result<&str, RuntimeError> {
        Ok(self.0.as_deref().unwrap_or(""))
    }
}

impl Basic for DateHour {
    fn data_type(&self) -> DataType {
        DataType::DateHour
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for Char {
    fn data_type(&self) -> DataType {
        DataType::Char
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }

    fn get_char(&self) -> Result<u8, RuntimeError> {
        self.0.map_or(Ok(u8::MIN), Ok)
    }
}

impl Basic for Short {
    fn data_type(&self) -> DataType {
        DataType::Short
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }

    fn get_short(&self) -> Result<i16, RuntimeError> {
        self.0.map_or(Ok(i16::MIN), Ok)
    }
}

impl Basic for Int {
    fn data_type(&self) -> DataType {
        DataType::Int
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }

    fn get_int(&self) -> Result<i32, RuntimeError> {
        self.0.map_or(Ok(i32::MIN), Ok)
    }
}

impl Basic for Long {
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }

    fn get_long(&self) -> Result<i64, RuntimeError> {
        self.0.map_or(Ok(i64::MIN), Ok)
    }
}

impl Basic for Float {
    fn data_type(&self) -> DataType {
        DataType::Float
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
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

    fn is_null(&self) -> bool {
        self.0.is_some()
    }

    fn get_double(&self) -> Result<f64, RuntimeError> {
        Ok(self
            .0
            .map_or(f64::MIN, |ordered_float| ordered_float.into_inner()))
    }
}
