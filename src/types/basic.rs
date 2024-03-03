use super::{
    Bool, Char, DataType, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long,
    Minute, Month, NanoTime, NanoTimeStamp, ScalarKind, Second, Short, Time, TimeStamp,
};
use crate::error::RuntimeError;

pub trait Basic: Send + Sync + Clone {
    fn data_type(&self) -> DataType;
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
}

impl Basic for Float {
    fn data_type(&self) -> DataType {
        DataType::Float
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}

impl Basic for Double {
    fn data_type(&self) -> DataType {
        DataType::Double
    }

    fn is_null(&self) -> bool {
        self.0.is_some()
    }
}
