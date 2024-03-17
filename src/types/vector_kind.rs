use std::fmt::{self, Display};
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::{scalar::for_all_branches, *};
use crate::{Deserialize, Serialize};

// implement VectorKind
#[derive(Debug, Clone)]
pub enum VectorKind {
    Void(Vector<()>),
    Bool(Vector<Bool>),
    Char(Vector<Char>),
    Short(Vector<Short>),
    Int(Vector<Int>),
    Long(Vector<Long>),
    Date(Vector<Date>),
    Month(Vector<Month>),
    Time(Vector<Time>),
    Minute(Vector<Minute>),
    Second(Vector<Second>),
    DateTime(Vector<DateTime>),
    TimeStamp(Vector<TimeStamp>),
    NanoTime(Vector<NanoTime>),
    NanoTimeStamp(Vector<NanoTimeStamp>),
    Float(Vector<Float>),
    Double(Vector<Double>),
    String(Vector<DolphinString>),
    DateHour(Vector<DateHour>),
}

impl VectorKind {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

macro_rules! dispatch_len {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub fn len(&self) -> usize {
                match self {
                    VectorKind::Void(s) => s.len(),
                    $(
                        VectorKind::$enum_name(s) => s.len(),
                    )*
                }
            }
        }
    };
}
for_all_branches!(dispatch_len);

// implement Basic trait
macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Basic for VectorKind {
            fn data_type(&self) -> DataType {
                match self {
                    VectorKind::Void(_) => DataType::Void,
                    $(
                        VectorKind::$enum_name(s) => s.data_type(),
                    )*
                }
            }

            fn data_category(&self) -> DataCategory {
                DataCategory::from_data_type(&self.data_type())
            }

            fn data_form(&self) -> DataForm {
                DataForm::Vector
            }

            fn size(&self) -> usize {
                self.len()
            }
        }
    };
}
for_all_branches!(dispatch_data_type);

// implement Serialize and Deserialize traits
impl Serialize for VectorKind {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        (self.data_type().to_u8(), self.data_form().to_u8()).serialize(buffer)?;

        buffer.put_i32(self.size() as i32);
        buffer.put_i32(1);

        self.serialize_data(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        (self.data_type().to_u8(), self.data_form().to_u8()).serialize_le(buffer)?;

        buffer.put_i32_le(self.size() as i32);
        buffer.put_i32(1);

        self.serialize_data_le(buffer)
    }
}

impl Deserialize for VectorKind {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32().await?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let _cols = reader.read_i32().await?;

        self.resize(len);
        self.deserialize_data(reader).await
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32_le().await?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let _cols = reader.read_i32_le().await?;

        self.resize(len);
        self.deserialize_data_le(reader).await
    }
}

// implement Display trait
impl Display for VectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorKind::Void(_) => write!(f, "()"),
            VectorKind::Bool(s) => write!(f, "{}", s),
            VectorKind::Char(s) => write!(f, "{}", s),
            VectorKind::Short(s) => write!(f, "{}", s),
            VectorKind::Int(s) => write!(f, "{}", s),
            VectorKind::Long(s) => write!(f, "{}", s),
            VectorKind::Date(s) => write!(f, "{}", s),
            VectorKind::Month(s) => write!(f, "{}", s),
            VectorKind::Time(s) => write!(f, "{}", s),
            VectorKind::Minute(s) => write!(f, "{}", s),
            VectorKind::Second(s) => write!(f, "{}", s),
            VectorKind::DateTime(s) => write!(f, "{}", s),
            VectorKind::TimeStamp(s) => write!(f, "{}", s),
            VectorKind::NanoTime(s) => write!(f, "{}", s),
            VectorKind::NanoTimeStamp(s) => write!(f, "{}", s),
            VectorKind::Float(s) => write!(f, "{}", s),
            VectorKind::Double(s) => write!(f, "{}", s),
            VectorKind::String(s) => write!(f, "{}", s),
            VectorKind::DateHour(s) => write!(f, "{}", s),
        }
    }
}

// create a vector from data type
macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    DataType::Void => Some(Self::Void(Vector::new())),
                    $(
                        DataType::$struct_name => Some(Self::$enum_name(Vector::new())),
                    )*
                    _ => None,
                }
            }
        }
    };
}
for_all_branches!(dispatch_reflect);

// implement resize
macro_rules! dispatch_resize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub fn resize(&mut self, new_len: usize) {
                match self {
                    VectorKind::Void(s) => s.resize(new_len, ()),
                    $(
                        VectorKind::$enum_name(s) => s.resize(new_len, $struct_name::default()),
                    )*
                }
            }
        }
    };
}
for_all_branches!(dispatch_resize);
