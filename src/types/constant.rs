use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::{
    dictionary::DictionaryKind, pair::PairKind, scalar::ScalarKind, set::SetKind,
    vector::VectorKind, Short, Vector,
};
use crate::{Deserialize, Serialize};

pub trait Constant: Send + Sync + Clone {
    /// data category identifier for serialization.
    fn data_category(&self) -> u8;

    /// Returns the number of elements in [`Constant`].
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

#[derive(Debug, Clone)]
pub enum ConstantKind {
    Scalar(ScalarKind),
    Vector(VectorKind),
    Pair(PairKind),
    Dictionary(DictionaryKind),
    Set(SetKind),
}

impl Default for ConstantKind {
    fn default() -> Self {
        Self::Scalar(ScalarKind::Void)
    }
}

impl ConstantKind {
    fn from_category(data_type: u8, data_form: u8) -> Option<Self> {
        match data_form {
            0 => ScalarKind::from_type(data_type).map(Self::Scalar),
            1 => VectorKind::from_type(data_type).map(Self::Vector),
            2 => PairKind::from_type(data_type).map(Self::Pair),
            4 => Some(Self::Set(HashSet::new())),
            5 => Some(Self::Dictionary(HashMap::new())),
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

macro_rules! for_all_constants {
    ($macro:tt) => {
        $macro!(
            (Scalar, ScalarKind),
            (Vector, VectorKind),
            (Pair, PairKind),
            (Set, SetKind),
            (Dictionary, DictionaryKind)
        );
    };
}

for_all_constants!(try_from_impl);

for_all_constants!(dispatch_serialize);

for_all_constants!(dispatch_deserialize);
