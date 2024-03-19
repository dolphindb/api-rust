use std::collections::HashMap;
use std::fmt::{self, Display};
use std::io::{Error, ErrorKind};
use tokio::io::AsyncBufReadExt;

use super::{
    constant::ConstantKind, scalar::ScalarKind, Basic, DataCategory, DataForm, DataType, VectorKind,
};
use crate::error::RuntimeError;
use crate::{Deserialize, Serialize};

// implement Dictionary
#[derive(Debug, Clone)]
pub struct Dictionary {
    data: HashMap<ScalarKind, ConstantKind>,
    key_type: DataType,
    value_type: DataType,
}

impl Dictionary {
    pub fn new(key_type: DataType, value_type: DataType) -> Self {
        Self {
            data: HashMap::new(),
            key_type,
            value_type,
        }
    }

    pub fn insert(&mut self, key: ScalarKind, value: ConstantKind) -> Result<(), RuntimeError> {
        if key.data_type() != self.key_type {
            Err(RuntimeError::InvalidKeyType)
        } else {
            self.data.insert(key, value);
            Ok(())
        }
    }

    // pub fn keys(dict: &Dictionary) ->  {
    //     // todo: borrow?
    //     todo!()
    // }

    // pub fn values(dict: &Dictionary) -> Result<VectorKind, ()> {
    //     // todo: borrow?
    //     let values = dict.data.values().cloned().collect::<Vec<_>>();
    //     values.try_into().map_err(|_| ())
    // }

    // TODO: optimize
    pub(crate) fn from_type(value_type: DataType) -> Option<Self> {
        Some(Self {
            data: HashMap::new(),
            key_type: DataType::Void,
            value_type,
        })
    }
}

// TODO: optimize
pub fn from_vectors(keys: VectorKind, values: VectorKind) -> Result<Dictionary, ()> {
    // the keys of dictionary must have a certain type but the values of dictionary can be an any vector

    // TODO
    // if keys.data_type() == DataType::Any {
    // Err
    // }

    // todo: optimize
    let key_type = keys.data_type();
    let value_type = values.data_type();
    let keys: Vec<ScalarKind> = keys.into();
    let values: Vec<ScalarKind> = values.into();

    if keys.len() != values.len() {
        return Err(());
    }

    let data = keys
        .into_iter()
        .zip(values.into_iter().map(ConstantKind::Scalar))
        .collect::<HashMap<_, _>>();
    Ok(Dictionary {
        data,
        key_type,
        value_type,
    })
}

pub(crate) fn dictionary_keys(dict: &Dictionary) -> Result<VectorKind, ()> {
    // todo: borrow?
    let keys = dict.data.keys().cloned().collect::<Vec<_>>();
    keys.try_into().map_err(|_| ())
}

pub(crate) fn dictionary_values(dict: &Dictionary) -> Result<VectorKind, ()> {
    // todo: borrow?
    let values = dict.data.values().cloned().collect::<Vec<_>>();
    values.try_into().map_err(|_| ())
}

impl Serialize for Dictionary {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = dictionary_keys(self)?;
        let values = dictionary_values(self)?;

        (values.data_type().to_u8(), self.data_form().to_u8()).serialize(buffer)?;

        keys.serialize(buffer)?;
        values.serialize(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = dictionary_keys(self)?;
        let values = dictionary_values(self)?;

        (values.data_type().to_u8(), self.data_form().to_u8()).serialize_le(buffer)?;

        keys.serialize_le(buffer)?;
        values.serialize_le(buffer)?;

        Ok(0)
    }
}

impl Deserialize for Dictionary {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut type_form = (0u8, 0u8);
        type_form.deserialize(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != DataForm::Vector.to_u8() {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let data_type = DataType::from_u8(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;
        let mut keys = VectorKind::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        keys.deserialize(reader).await?;

        let mut type_form = (0u8, 0u8);
        type_form.deserialize(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != DataForm::Vector.to_u8() {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let data_type = DataType::from_u8(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;
        let mut values = VectorKind::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        values.deserialize(reader).await?;

        *self = from_vectors(keys, values)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut type_form = (0u8, 0u8);
        type_form.deserialize_le(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != DataForm::Vector.to_u8() {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let data_type = DataType::from_u8(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;
        let mut keys = VectorKind::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        keys.deserialize_le(reader).await?;

        let mut type_form = (0u8, 0u8);
        type_form.deserialize_le(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != DataForm::Vector.to_u8() {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let data_type = DataType::from_u8(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;
        let mut values = VectorKind::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        values.deserialize_le(reader).await?;

        *self = from_vectors(keys, values)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        Ok(())
    }
}

// implement Basic trait for Dictionary
impl Basic for Dictionary {
    fn data_type(&self) -> DataType {
        self.value_type
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::from_data_type(&self.value_type)
    }

    fn data_form(&self) -> DataForm {
        DataForm::Dictionary
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

// implement Display trait for Dictionary
impl Display for Dictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (key, value) in self.data.iter() {
            writeln!(f, "{}->{}", key, value)?;
        }
        Ok(())
    }
}
