use std::fmt::Display;

use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};

use super::*;

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use paste::paste;

pub trait Constant: Send + Sync + Clone {
    /// data category identifier for serialization.
    fn data_form(&self) -> DataForm;

    /// data type identifier for serialization.
    fn data_type(&self) -> DataType;

    /// Returns the number of elements in [`Constant`].
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstantImpl {
    Scalar(ScalarImpl),
    Vector(VectorImpl),
    Pair(PairImpl),
    Dictionary(DictionaryImpl),
    Set(SetImpl),
    Table(Table),
}

impl Default for ConstantImpl {
    fn default() -> Self {
        Self::Scalar(ScalarImpl::default())
    }
}

impl ConstantImpl {
    pub fn is_null(&self) -> bool {
        match self {
            ConstantImpl::Scalar(v) => v.is_null(),
            _ => false,
        }
    }

    fn from_category(data_type: DataType, data_form: DataForm) -> Option<Self> {
        match data_form {
            DataForm::Scalar => ScalarImpl::from_type(data_type).map(Self::Scalar),
            DataForm::Vector => VectorImpl::from_type(data_type).map(Self::Vector),
            DataForm::Pair => PairImpl::from_type(data_type).map(Self::Pair),
            DataForm::Set => SetImpl::from_type(data_type).map(Self::Set),
            DataForm::Dictionary => DictionaryImpl::from_type(data_type).map(Self::Dictionary),
            DataForm::Table => Some(Self::Table(Table::default())),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<ConstantImpl> {
        match self {
            ConstantImpl::Vector(ref v) => v.get(index),
            _ => None,
        }
    }
}

impl Serialize for (u8, u8) {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        Short::from(i16::from_le_bytes([self.0, self.1])).serialize(buffer)?;
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        Short::from(i16::from_le_bytes([self.0, self.1])).serialize_le(buffer)?;
        Ok(0)
    }
}

impl Deserialize for (u8, u8) {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let packed = reader.read_i16().await?.to_le_bytes();
        self.0 = packed[0];
        self.1 = packed[1];

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
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
        impl From<$struct_name> for ConstantImpl {
            fn from(value: $struct_name) -> Self {
                Self::$enum_name(value)
            }
        }

        impl TryFrom<ConstantImpl> for $struct_name {
            type Error = Error;

            fn try_from(value: ConstantImpl) -> Result<Self, Self::Error> {
                match value {
                    ConstantImpl::$enum_name(value) => Ok(value),
                    _ => Err(Error::InvalidConvert {
                        from: value.data_form().to_string(),
                        to: $struct_name::data_form().to_string(),
                    }),
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
        impl Serialize for ConstantImpl {
            fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
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
        impl Deserialize for ConstantImpl {
            async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                let mut type_form = (0u8, 0u8);
                type_form.deserialize(reader).await?;

                if type_form.0 == 128 + DataType::Symbol as u8 {
                    let mut s = Vector::<Symbol>::new();
                    s.deserialize_with_symbol_base(reader).await?;
                    *self = s.into();
                    return Ok(());
                }

                let mut data_type = type_form.0.try_into()?;
                let data_form = type_form.1.try_into()?;

                // println!("data type: {}, data form: {}", data_type, data_form);

                if data_form == DataForm::Dictionary {
                    // discard useless value type
                    data_type = DataType::Bool;
                }

                *self = Self::from_category(data_type, data_form)
                    .ok_or(Error::Unsupported{data_form: data_form.to_string(), data_type: data_type.to_string()})?;

                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize(reader).await,
                    )*
                }
            }

            async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                let mut type_form = (0u8, 0u8);
                type_form.deserialize_le(reader).await?;

                if type_form.0 == 128 + DataType::Symbol as u8 {
                    let mut s = Vector::<Symbol>::new();
                    s.deserialize_with_symbol_base_le(reader).await?;
                    *self = s.into();
                    return Ok(());
                }

                let mut data_type = type_form.0.try_into()?;
                let data_form = type_form.1.try_into()?;

                // println!("data type: {}, data form: {}", data_type, data_form);

                if data_form == DataForm::Dictionary {
                    // discard useless value type
                    data_type = DataType::Bool;
                }

                *self = Self::from_category(data_type, data_form)
                    .ok_or(Error::Unsupported{data_form: data_form.to_string(), data_type: data_type.to_string()})?;

                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_into_any {
    ($enum_name:ident, $struct_name:ident) => {
        impl From<$struct_name> for Any {
            fn from(value: $struct_name) -> Self {
                let c: ConstantImpl = value.into();
                c.into()
            }
        }
    };

    ($(($enum_name:ident, $struct_name:ident)), *) => {
        $(
            dispatch_into_any!($enum_name, $struct_name);
        )*
    };
}

macro_rules! dispatch_impl_constant {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Constant for ConstantImpl {
            fn data_form(&self) -> DataForm {
                match self {
                    $(
                        Self::$enum_name(s) => s.data_form(),
                    )*
                }
            }

            fn data_type(&self) -> DataType {
                match self {
                    $(
                        Self::$enum_name(s) => s.data_type(),
                    )*
                }
            }

            fn len(&self) -> usize {
                match self {
                    $(
                        Self::$enum_name(s) => s.len(),
                    )*
                }
            }
        }
    }
}

macro_rules! dispatch_as {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        paste! {
            impl ConstantImpl {
                $(
                    pub fn [<as_ $enum_name:lower>](&self) -> Result<&$struct_name, Error> {
                        match self {
                            ConstantImpl::$enum_name(v) => Ok(v),
                            _ => Err(Error::InvalidConvert {
                                from: self.data_form().to_string(),
                                to: $struct_name::data_form().to_string(),
                            }),
                        }
                    }

                    pub fn [<as_mut_ $enum_name:lower>](&mut self) -> Result<&mut $struct_name, Error> {
                        match self {
                            ConstantImpl::$enum_name(v) => Ok(v),
                            _ => Err(Error::InvalidConvert {
                                from: self.data_form().to_string(),
                                to: $struct_name::data_form().to_string(),
                            }),
                        }
                    }
                )*
            }
        }
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for ConstantImpl {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        ConstantImpl::$enum_name(val) => write!(f, "{}", val),
                    )*
                }
            }
        }
    };
}

macro_rules! for_all_constants {
    ($macro:tt) => {
        $macro!(
            (Scalar, ScalarImpl),
            (Vector, VectorImpl),
            (Pair, PairImpl),
            (Set, SetImpl),
            (Dictionary, DictionaryImpl),
            (Table, Table)
        );
    };
}

for_all_constants!(try_from_impl);

for_all_constants!(dispatch_serialize);

for_all_constants!(dispatch_deserialize);

for_all_constants!(dispatch_into_any);

for_all_constants!(dispatch_impl_constant);

for_all_constants!(dispatch_as);

for_all_constants!(dispatch_display);
