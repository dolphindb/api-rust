use std::{
    collections::HashSet,
    io::{Error, ErrorKind},
};
use tokio::io::AsyncBufReadExt;

use crate::{Deserialize, Serialize};

use super::{constant::Constant, ScalarImpl, VectorImpl};

pub type SetImpl = HashSet<ScalarImpl>;

impl Constant for SetImpl {
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

impl From<VectorImpl> for SetImpl {
    fn from(value: VectorImpl) -> Self {
        let s: Vec<ScalarImpl> = value.into();
        s.into_iter().collect::<HashSet<_>>()
    }
}

pub(crate) fn set_keys(set: &SetImpl) -> Result<VectorImpl, ()> {
    let keys = set.iter().cloned().collect::<Vec<_>>();
    keys.try_into()
}

impl Serialize for SetImpl {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = set_keys(self)?;

        (keys.data_type(), self.data_category()).serialize(buffer)?;

        keys.serialize(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = set_keys(self)?;

        (keys.data_type(), self.data_category()).serialize(buffer)?;

        keys.serialize(buffer)?;

        Ok(0)
    }
}

impl Deserialize for SetImpl {
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

        let mut v = VectorImpl::from_type(data_type)
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
        if data_form != VectorImpl::FORM_BYTE {
            return Err(Error::new(ErrorKind::InvalidData, "expect vector."));
        }

        let mut v = VectorImpl::from_type(data_type)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown data type."))?;

        v.deserialize_le(reader).await?;

        *self = v.into();

        Ok(())
    }
}
