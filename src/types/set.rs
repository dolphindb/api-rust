use std::fmt::{self, Display};
use std::{
    collections::HashSet,
    io::{Error, ErrorKind},
};
use tokio::io::AsyncBufReadExt;

use super::{Basic, DataCategory, DataForm, DataType, ScalarKind, VectorKind};
use crate::{error::RuntimeError, Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Set {
    data: HashSet<ScalarKind>,
    data_type: DataType,
}

impl Set {
    pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
        Some(Set {
            data: HashSet::new(),
            data_type,
        })
    }
}

impl From<VectorKind> for Set {
    fn from(value: VectorKind) -> Self {
        let data_type = value.data_type();
        let s: Vec<ScalarKind> = value.into();
        Self {
            data: s.into_iter().collect::<HashSet<_>>(),
            data_type,
        }
    }
}

pub(crate) fn set_keys(set: &Set) -> Result<VectorKind, RuntimeError> {
    let keys = set.data.iter().cloned().collect::<Vec<_>>();
    keys.try_into()
}

impl Serialize for Set {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = set_keys(self).map_err(|_| ())?;

        (keys.data_type().to_u8(), self.data_form().to_u8()).serialize(buffer)?;

        keys.serialize(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let keys = set_keys(self).map_err(|_| ())?;

        (keys.data_type().to_u8(), self.data_form().to_u8()).serialize(buffer)?;

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
        if data_form != DataForm::Vector.to_u8() {
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
        if data_form != DataForm::Vector.to_u8() {
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
        self.data_type
    }

    fn data_category(&self) -> DataCategory {
        DataCategory::from_data_type(&self.data_type)
    }

    fn data_form(&self) -> DataForm {
        DataForm::Set
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

// implement Display trait for Set
impl Display for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, e) in self.data.iter().enumerate() {
            if idx == 0 {
                write!(f, "[{}", e)?;
            } else {
                write!(f, ", {}", e)?;
            }
        }
        write!(f, "]")
    }
}
