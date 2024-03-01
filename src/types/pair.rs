use std::io::{Error, ErrorKind};

use crate::{Deserialize, Serialize};

use super::{constant::Constant, scalar::ScalarKind, Basic, VectorKind};

use tokio::io::AsyncBufReadExt;

#[derive(Debug, Clone, Default)]
pub struct Pair {
    first: ScalarKind,
    second: ScalarKind,
    data_type: u8,
}

impl Pair {
    pub const FORM_BYTE: u8 = 1;

    pub fn new(pair: (ScalarKind, ScalarKind)) -> Self {
        let data_type = pair.0.data_type();
        Self {
            first: pair.0,
            second: pair.1,
            data_type,
        }
    }

    pub(crate) fn from_type(data_type: u8) -> Option<Self> {
        ScalarKind::from_type(data_type)
            .zip(ScalarKind::from_type(data_type))
            .map(|(first, second)| Self {
                first,
                second,
                data_type,
            })
    }

    pub fn data_type(&self) -> u8 {
        self.data_type
    }

    pub fn first(&self) -> &ScalarKind {
        &self.first
    }

    pub fn second(&self) -> &ScalarKind {
        &self.second
    }

    pub fn first_mut(&mut self) -> &mut ScalarKind {
        &mut self.first
    }

    pub fn second_mut(&mut self) -> &mut ScalarKind {
        &mut self.second
    }
}

impl Constant for Pair {
    fn data_category(&self) -> u8 {
        Self::FORM_BYTE
    }

    fn len(&self) -> usize {
        2
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl TryFrom<Pair> for VectorKind {
    type Error = ();

    fn try_from(value: Pair) -> Result<Self, Self::Error> {
        if value.first.data_type() != value.second.data_type() {
            Err(())
        } else {
            let v = vec![value.first, value.second];
            TryInto::<VectorKind>::try_into(v)
        }
    }
}

impl TryFrom<VectorKind> for Pair {
    type Error = ();

    fn try_from(value: VectorKind) -> Result<Self, Self::Error> {
        if value.len() != 2 {
            return Err(());
        }

        let data_type = value.data_type();
        let mut s: Vec<ScalarKind> = value.into();
        Ok(Pair {
            first: s.pop().unwrap(),
            second: s.pop().unwrap(),
            data_type,
        })
    }
}

impl Serialize for Pair {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let v: VectorKind = self.clone().try_into()?;
        (v.data_type(), self.data_category()).serialize(buffer)?;

        buffer.put_i32(self.len() as i32);
        buffer.put_i32(1);

        v.serialize_data(buffer)?;
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let v: VectorKind = self.clone().try_into()?;
        (v.data_type(), self.data_category()).serialize_le(buffer)?;

        buffer.put_i32_le(self.len() as i32);
        buffer.put_i32_le(1);

        v.serialize_data(buffer)?;
        Ok(0)
    }
}

impl Deserialize for Pair {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut v =
            VectorKind::from_type(self.data_type).ok_or(Error::new(ErrorKind::InvalidData, ""))?;
        v.deserialize(reader).await?;

        *self = v
            .try_into()
            .map_err(|_| Error::new(ErrorKind::InvalidData, ""))?;

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut v =
            VectorKind::from_type(self.data_type).ok_or(Error::new(ErrorKind::InvalidData, ""))?;
        v.deserialize_le(reader).await?;

        *self = v
            .try_into()
            .map_err(|_| Error::new(ErrorKind::InvalidData, ""))?;

        Ok(())
    }
}

// implement Basic trait for Pair
impl Basic for Pair {
    fn data_type(&self) -> u8 {
        self.data_type()
    }
}
