use std::io::{Error, ErrorKind};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::{
    scalar::ScalarKind, vector::Vector, Basic, DataCategory, DataForm, DataType, Dictionary, Pair,
    Set, Short, VectorKind,
};
use crate::{error::RuntimeError, Deserialize, Serialize};

pub trait Constant: Send + Sync + Clone {
    fn is_empty(&self) -> bool;
}

#[derive(Debug, Clone)]
pub enum ConstantKind {
    Scalar(ScalarKind),
    Vector(VectorKind),
    Pair(Pair),
    Set(Set),
    Dictionary(Dictionary),
}

impl Default for ConstantKind {
    fn default() -> Self {
        Self::Scalar(ScalarKind::Void)
    }
}

impl ConstantKind {
    fn from_category(data_type: u8, data_form: u8) -> Option<Self> {
        let data_type = DataType::from_u8(data_type)?;
        match data_form {
            0 => ScalarKind::from_type(data_type).map(Self::Scalar),
            1 => VectorKind::from_type(data_type).map(Self::Vector),
            2 => Pair::from_type(data_type).map(Self::Pair),
            4 => Set::from_type(data_type).map(Self::Set),
            5 => Dictionary::from_type(data_type).map(Self::Dictionary),
            _ => None,
        }
    }
}

impl TryFrom<Vec<ConstantKind>> for VectorKind {
    type Error = ();

    fn try_from(value: Vec<ConstantKind>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Ok(VectorKind::Void(Vector::new()));
        }

        // todo(bureaucratic): any vector?

        let scalars: Result<Vec<ScalarKind>, ()> =
            value.into_iter().map(|c| c.try_into()).collect();
        scalars?.try_into()
    }
}

impl Serialize for (u8, u8) {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        Short::from(i16::from_le_bytes([self.0, self.1])).serialize(buffer)?;
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        Short::from(i16::from_le_bytes([self.0, self.1])).serialize_le(buffer)?;
        Ok(0)
    }
}

impl Deserialize for (u8, u8) {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let packed = reader.read_i16().await?.to_le_bytes();
        self.0 = packed[0];
        self.1 = packed[1];

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let packed = reader.read_i16_le().await?.to_le_bytes();
        self.0 = packed[0];
        self.1 = packed[1];

        Ok(())
    }
}

macro_rules! for_all_constants {
    ($macro:tt) => {
        $macro!(
            (Scalar, ScalarKind),
            (Vector, VectorKind),
            (Pair, Pair),
            (Set, Set),
            (Dictionary, Dictionary)
        );
    };
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Serialize for ConstantKind {
            fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.serialize_le(buffer),
                    )*
                }
            }
        }
    };
}

for_all_constants!(dispatch_serialize);

macro_rules! dispatch_deserialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Deserialize for ConstantKind {
            async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                let mut type_form = (0u8, 0u8);
                type_form.deserialize(reader).await?;

                let (data_type, data_form) = type_form;

                #[cfg(feature = "debug_pr")]
                println!("data type: {}, data form: {}", data_type, data_form);

                *self = Self::from_category(data_type, data_form)
                    .ok_or(Error::new(ErrorKind::InvalidData, ""))?;

                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize(reader).await,
                    )*
                }
            }

            async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                let mut type_form = (0u8, 0u8);
                type_form.deserialize_le(reader).await?;

                let (data_type, data_form) = type_form;

                #[cfg(feature = "debug_pr")]
                println!("data type: {}, data form: {}", data_type, data_form);

                *self = Self::from_category(data_type, data_form)
                    .ok_or(Error::new(ErrorKind::InvalidData, ""))?;

                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                }
            }
        }
    };
}
for_all_constants!(dispatch_deserialize);

macro_rules! try_from_impl {
    ($enum_name:ident, $struct_name:ident) => {
        impl From<$struct_name> for ConstantKind {
            fn from(value: $struct_name) -> Self {
                Self::$enum_name(value)
            }
        }

        impl TryFrom<ConstantKind> for $struct_name {
            type Error = ();

            fn try_from(value: ConstantKind) -> Result<Self, Self::Error> {
                match value {
                    ConstantKind::$enum_name(value) => Ok(value),
                    _ => Err(()),
                }
            }
        }
    };

    ($(($enum_name:ident, $struct_name:ident)), *) => {
        $(
            try_from_impl!($enum_name, $struct_name);
        )*
    };
}
for_all_constants!(try_from_impl);

// implement Basic trait for ConstantKind
impl Basic for ConstantKind {
    fn data_type(&self) -> DataType {
        match self {
            ConstantKind::Scalar(obj) => obj.data_type(),
            ConstantKind::Vector(obj) => obj.data_type(),
            ConstantKind::Pair(obj) => obj.data_type(),
            ConstantKind::Set(obj) => obj.data_type(),
            ConstantKind::Dictionary(obj) => obj.data_type(),
        }
    }

    fn data_category(&self) -> DataCategory {
        match self {
            ConstantKind::Scalar(obj) => obj.data_category(),
            ConstantKind::Vector(obj) => obj.data_category(),
            ConstantKind::Pair(obj) => obj.data_category(),
            ConstantKind::Set(obj) => obj.data_category(),
            ConstantKind::Dictionary(obj) => obj.data_category(),
        }
    }

    fn is_null(&self) -> Result<bool, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.is_null(),
            _ => Err(RuntimeError::NotSupportInterface),
        }
    }

    fn data_form(&self) -> DataForm {
        match self {
            ConstantKind::Scalar(obj) => obj.data_form(),
            ConstantKind::Vector(obj) => obj.data_form(),
            ConstantKind::Pair(obj) => obj.data_form(),
            ConstantKind::Set(obj) => obj.data_form(),
            ConstantKind::Dictionary(obj) => obj.data_form(),
        }
    }

    fn size(&self) -> usize {
        match self {
            ConstantKind::Scalar(obj) => obj.size(),
            ConstantKind::Vector(obj) => obj.size(),
            ConstantKind::Pair(obj) => obj.size(),
            ConstantKind::Set(obj) => obj.size(),
            ConstantKind::Dictionary(obj) => obj.size(),
        }
    }

    // implementation of Basic getters
    fn get_bool(&self) -> Result<bool, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_bool(),
            _ => Err(RuntimeError::NotBoolScalar),
        }
    }
    fn get_char(&self) -> Result<u8, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_char(),
            _ => Err(RuntimeError::NotCharScalar),
        }
    }
    fn get_short(&self) -> Result<i16, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_short(),
            _ => Err(RuntimeError::NotShortScalar),
        }
    }
    fn get_int(&self) -> Result<i32, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_int(),
            _ => Err(RuntimeError::NotIntScalar),
        }
    }
    fn get_long(&self) -> Result<i64, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_long(),
            _ => Err(RuntimeError::NotLongScalar),
        }
    }
    fn get_float(&self) -> Result<f32, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_float(),
            _ => Err(RuntimeError::NotFloatScalar),
        }
    }
    fn get_double(&self) -> Result<f64, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_double(),
            _ => Err(RuntimeError::NotDoubleScalar),
        }
    }
    fn get_string(&self) -> Result<&str, RuntimeError> {
        match self {
            ConstantKind::Scalar(obj) => obj.get_string(),
            _ => Err(RuntimeError::NotStringScalar),
        }
    }
}
