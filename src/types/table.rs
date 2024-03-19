use std::fmt::{self, Display};
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::{Basic, ConstantKind, DataCategory, DataForm, DataType, VectorKind};
use crate::{error::RuntimeError, Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    columns: Vec<VectorKind>,
    column_names: Vec<String>,
}

impl Table {
    pub fn new(
        name: String,
        columns: Vec<VectorKind>,
        column_names: Vec<String>,
    ) -> Result<Self, RuntimeError> {
        // todo check
        Ok(Self {
            name,
            columns,
            column_names,
        })
    }

    pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
        Some(Table {
            name: String::new(),
            columns: Vec::new(),
            column_names: Vec::new(),
        })
    }
}

impl From<Vec<VectorKind>> for Table {
    fn from(value: Vec<VectorKind>) -> Self {
        todo!()
    }
}

impl Serialize for Table {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        // let v: VectorKind = self.clone().try_into().map_err(|_| ())?;
        // (v.data_type().to_u8(), self.data_form().to_u8()).serialize(buffer)?;

        // buffer.put_i32(self.size() as i32);
        // buffer.put_i32(1);

        // v.serialize_data(buffer)?;
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        // let v: VectorKind = self.clone().try_into().map_err(|_| ())?;
        // (v.data_type().to_u8(), self.data_form().to_u8()).serialize_le(buffer)?;

        // buffer.put_i32_le(self.size() as i32);
        // buffer.put_i32_le(1);

        // v.serialize_data(buffer)?;
        Ok(0)
    }
}

pub(crate) async fn read_string<R>(reader: &mut R) -> std::io::Result<String>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut msg = Vec::new();
    if reader.read_until(0, &mut msg).await? == 0 {
        return Err(Error::from(ErrorKind::UnexpectedEof));
    }
    msg.pop();
    match String::from_utf8(msg) {
        Ok(str) => Ok(str),
        Err(err) => Err(Error::new(ErrorKind::InvalidData, err.to_string())),
    }
}

impl Deserialize for Table {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let rows = reader.read_i32().await?;
        let cols = reader.read_i32().await?;
        let table_name = read_string(reader).await?; // TODO
        let mut column_names = Vec::new();
        for _ in 0..cols {
            column_names.push(read_string(reader).await?)
        }

        let mut columns = Vec::new();
        for _ in 0..cols {
            let flag = reader.read_i16().await?;
            let data_form = DataForm::from_u8((flag >> 8) as u8).ok_or(Error::new(
                ErrorKind::Other,
                format!(
                    "Fail to deserialize table, not support data form {}.",
                    flag >> 8
                ),
            ))?;
            if data_form != DataForm::Vector {
                return Err(Error::new(ErrorKind::Other, "Fail to deserialize table."));
            }
            let data_type = DataType::from_u8((flag & 0xff) as u8).ok_or(Error::new(
                ErrorKind::Other,
                format!(
                    "Fail to deserialize table, not support data type {}.",
                    flag & 0xff
                ),
            ))?;

            let column = ConstantKind::create_dummy_obj(data_type.to_u8(), data_form.to_u8())
                .ok_or(Error::new(ErrorKind::Other, "Fail to deserialize table."))?;
            let mut column = VectorKind::try_from(column)
                .map_err(|_| Error::new(ErrorKind::Other, "Fail to deserialize table."))?;
            column.deserialize(reader).await?;
            if (column.size() as i32) != rows {
                return Err(Error::new(ErrorKind::Other, "Fail to deserialize table."));
            }

            println!("{column}");
            columns.push(column);
        }

        *self = Table::new(table_name, columns, column_names)
            .map_err(|_| Error::new(ErrorKind::Other, "Fail to deserialize table."))?;
        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let rows = reader.read_i32_le().await?;
        let cols = reader.read_i32_le().await?;
        let table_name = read_string(reader).await?; // TODO
        let mut column_names = Vec::new();
        for _ in 0..cols {
            column_names.push(read_string(reader).await?)
        }

        let mut columns = Vec::new();
        for _ in 0..cols {
            let flag = reader.read_i16_le().await?;
            let data_form = DataForm::from_u8((flag >> 8) as u8).ok_or(Error::new(
                ErrorKind::Other,
                format!(
                    "Fail to deserialize table, not support data form {}.",
                    flag >> 8
                ),
            ))?;
            if data_form != DataForm::Vector {
                return Err(Error::new(ErrorKind::Other, "Fail to deserialize table."));
            }
            let data_type = DataType::from_u8((flag & 0xff) as u8).ok_or(Error::new(
                ErrorKind::Other,
                format!(
                    "Fail to deserialize table, not support data type {}.",
                    flag & 0xff
                ),
            ))?;

            let column = ConstantKind::create_dummy_obj(data_type.to_u8(), data_form.to_u8())
                .ok_or(Error::new(ErrorKind::Other, "Fail to deserialize table."))?;
            let mut column = VectorKind::try_from(column)
                .map_err(|_| Error::new(ErrorKind::Other, "Fail to deserialize table."))?;
            column.deserialize_le(reader).await?;
            if (column.size() as i32) != rows {
                return Err(Error::new(ErrorKind::Other, "Fail to deserialize table."));
            }

            println!("{column}");
            columns.push(column);
        }

        *self = Table::new(table_name, columns, column_names)
            .map_err(|_| Error::new(ErrorKind::Other, "Fail to deserialize table."))?;
        Ok(())
    }
}

// implement Basic trait for Table
impl Basic for Table {
    fn data_type(&self) -> DataType {
        todo!()
    }

    fn data_category(&self) -> DataCategory {
        todo!()
    }

    fn data_form(&self) -> DataForm {
        DataForm::Table
    }

    fn size(&self) -> usize {
        todo!()
    }
}

// implement Display trait for Pair
impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
