use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use tokio::io::AsyncBufReadExt;

use crate::{Deserialize, Serialize};

use super::{
    constant::{Constant, ConstantImpl},
    scalar::ScalarImpl,
    VectorImpl,
};

pub type DictionaryImpl = HashMap<ScalarImpl, ConstantImpl>;

impl Constant for DictionaryImpl {
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

pub(crate) fn dictionary_keys(dict: &DictionaryImpl) -> Result<VectorImpl, ()> {
    let keys = dict.iter().map(|(k, _v)| k.clone()).collect::<Vec<_>>();
    keys.try_into()
}

pub(crate) fn dictionary_values(dict: &DictionaryImpl) -> Result<VectorImpl, ()> {
    let values = dict.iter().map(|(_k, v)| v.clone()).collect::<Vec<_>>();
    values.try_into()
}

pub(crate) fn from_vectors(keys: VectorImpl, values: VectorImpl) -> Result<DictionaryImpl, ()> {
    let keys: Vec<ScalarImpl> = keys.into();
    let values: Vec<ScalarImpl> = values.into();

    if keys.len() != values.len() {
        return Err(());
    }

    let dict = keys
        .into_iter()
        .zip(values.into_iter().map(ConstantImpl::Scalar))
        .collect::<HashMap<_, _>>();

    Ok(dict)
}

impl Serialize for DictionaryImpl {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = dictionary_keys(self)?;
        let values = dictionary_values(self)?;

        (values.data_type(), self.data_category()).serialize(buffer)?;

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

        (values.data_type(), self.data_category()).serialize_le(buffer)?;

        keys.serialize_le(buffer)?;
        values.serialize_le(buffer)?;

        Ok(0)
    }
}

impl Deserialize for DictionaryImpl {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut type_form = (0u8, 0u8);
        type_form.deserialize(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != VectorImpl::FORM_BYTE {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let mut keys = VectorImpl::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        keys.deserialize(reader).await?;

        let mut type_form = (0u8, 0u8);
        type_form.deserialize(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != VectorImpl::FORM_BYTE {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let mut values = VectorImpl::from_type(data_type)
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

        if data_form != VectorImpl::FORM_BYTE {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let mut keys = VectorImpl::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        keys.deserialize_le(reader).await?;

        let mut type_form = (0u8, 0u8);
        type_form.deserialize_le(reader).await?;

        let (data_type, data_form) = type_form;

        if data_form != VectorImpl::FORM_BYTE {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let mut values = VectorImpl::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        values.deserialize_le(reader).await?;

        *self = from_vectors(keys, values)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        Ok(())
    }
}
