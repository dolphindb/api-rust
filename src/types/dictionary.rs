use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use tokio::io::AsyncBufReadExt;

use super::{
    constant::{Constant, ConstantKind},
    scalar::ScalarKind,
    Basic, DataType, VectorKind,
};
use crate::{Deserialize, Serialize};

pub type Dictionary = HashMap<ScalarKind, ConstantKind>;

impl Constant for Dictionary {
    fn data_category(&self) -> u8 {
        5
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

pub(crate) fn dictionary_keys(dict: &Dictionary) -> Result<VectorKind, ()> {
    let keys = dict.iter().map(|(k, _v)| k.clone()).collect::<Vec<_>>();
    keys.try_into()
}

pub(crate) fn dictionary_values(dict: &Dictionary) -> Result<VectorKind, ()> {
    let values = dict.iter().map(|(_k, v)| v.clone()).collect::<Vec<_>>();
    values.try_into()
}

pub(crate) fn from_vectors(keys: VectorKind, values: VectorKind) -> Result<Dictionary, ()> {
    let keys: Vec<ScalarKind> = keys.into();
    let values: Vec<ScalarKind> = values.into();

    if keys.len() != values.len() {
        return Err(());
    }

    let dict = keys
        .into_iter()
        .zip(values.into_iter().map(ConstantKind::Scalar))
        .collect::<HashMap<_, _>>();

    Ok(dict)
}

impl Serialize for Dictionary {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = dictionary_keys(self)?;
        let values = dictionary_values(self)?;

        (values.data_type().to_u8(), self.data_category()).serialize(buffer)?;

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

        (values.data_type().to_u8(), self.data_category()).serialize_le(buffer)?;

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

        if data_form != VectorKind::FORM_BYTE {
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

        if data_form != VectorKind::FORM_BYTE {
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

        if data_form != VectorKind::FORM_BYTE {
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

        if data_form != VectorKind::FORM_BYTE {
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
        DataType::Any
    }
}
