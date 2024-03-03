use std::{
    collections::HashSet,
    io::{Error, ErrorKind},
};
use tokio::io::AsyncBufReadExt;

use super::{constant::Constant, Basic, DataForm, DataType, ScalarKind, VectorKind};
use crate::{Deserialize, Serialize};

pub type Set = HashSet<ScalarKind>;

impl Constant for Set {
    fn data_category(&self) -> u8 {
        4
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl From<VectorKind> for Set {
    fn from(value: VectorKind) -> Self {
        let s: Vec<ScalarKind> = value.into();
        s.into_iter().collect::<HashSet<_>>()
    }
}

pub(crate) fn set_keys(set: &Set) -> Result<VectorKind, ()> {
    let keys = set.iter().cloned().collect::<Vec<_>>();
    keys.try_into()
}

impl Serialize for Set {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = set_keys(self)?;

        (keys.data_type().to_u8(), self.data_category()).serialize(buffer)?;

        keys.serialize(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = set_keys(self)?;

        (keys.data_type().to_u8(), self.data_category()).serialize(buffer)?;

        keys.serialize(buffer)?;

        Ok(0)
    }
}

impl Deserialize for Set {
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
        let mut v = VectorKind::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        v.deserialize(reader).await?;

        *self = v.into();

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
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
        let mut v = VectorKind::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        v.deserialize_le(reader).await?;

        *self = v.into();

        Ok(())
    }
}

// implement Basic trait for Set
impl Basic for Set {
    fn data_type(&self) -> DataType {
        DataType::Any
    }

    fn data_form(&self) -> DataForm {
        DataForm::Set
    }
}
