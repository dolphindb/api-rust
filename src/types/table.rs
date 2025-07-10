use std::fmt::Display;
use std::hash::Hash;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
};

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use prettytable::{Cell, Table as PrettyTable};

use crate::{
    error::{Error, Result},
    types::{DolphinString, Symbol},
    Deserialize, Serialize,
};

use super::{
    deserialize_vector, deserialize_vector_le, Constant, DataForm, DataType, Dictionary,
    DictionaryImpl, VectorImpl,
};

#[derive(Debug, Clone, Default)]
pub struct TableBuilder {
    name: String,
    columns: Vec<VectorImpl>,
    column_names: Vec<String>,
}

impl TableBuilder {
    // empty name is allowed in Table.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(&mut self, name: String) -> &mut Self {
        self.name = name;
        self
    }

    pub fn with_contents(
        &mut self,
        columns: Vec<VectorImpl>,
        column_names: Vec<String>,
    ) -> &mut Self {
        self.columns = columns;
        self.column_names = column_names;
        self
    }

    pub fn build(self) -> Result<Table> {
        if self.columns.len() != self.column_names.len() {
            return Err(Error::ConstraintsViolated(
                "mismatch columns and column names size".into(),
            ));
        }

        let len = if self.columns.is_empty() {
            0
        } else {
            self.columns.first().unwrap().len()
        };

        if self.columns.iter().any(|col| col.len() != len) {
            return Err(Error::ConstraintsViolated("mismatch columns size".into()));
        }

        if !no_duplicates(&self.column_names) {
            return Err(Error::ConstraintsViolated("duplicated column name".into()));
        }

        if self.column_names.iter().any(|n| n.is_empty()) {
            return Err(Error::ConstraintsViolated(
                "empty column name is not allowed".into(),
            ));
        }

        Ok(Table {
            name: self.name,
            columns: self.columns,
            column_names: self.column_names,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Table {
    name: String,
    columns: Vec<VectorImpl>,
    column_names: Vec<String>,
}

impl Table {
    pub fn len(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns.first().unwrap().len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn columns(&self) -> &Vec<VectorImpl> {
        &self.columns
    }

    pub fn column_names(&self) -> &Vec<String> {
        &self.column_names
    }

    /// Get the column in table with the index.
    pub fn get_columns_by_index(&self, index: usize) -> &VectorImpl {
        &self.columns[index]
    }

    /// Get the column in table with the column name. Return None if corresponding
    /// column doesn't exist.
    pub fn get_columns_by_name(&self, column_name: &str) -> Option<&VectorImpl> {
        self.column_names
            .iter()
            .position(|name| name == column_name)
            .map(|i| &self.columns[i])
    }

    /// Inserts a column at position `index`` within the table, shifting all columns after it to the right.
    pub fn insert_column(
        &mut self,
        column: VectorImpl,
        column_name: String,
        index: usize,
    ) -> Result<()> {
        let len = if self.columns.is_empty() {
            0
        } else {
            self.columns.first().unwrap().len()
        };

        if column.len() != len {
            return Err(Error::ConstraintsViolated("mismatch column size".into()));
        }

        if self.column_names.iter().any(|n| n == &column_name) {
            return Err(Error::ConstraintsViolated("duplicated column name".into()));
        }

        if column_name.is_empty() {
            return Err(Error::ConstraintsViolated(
                "empty column name is not allowed".into(),
            ));
        }

        if index > self.columns.len() {
            return Err(Error::ConstraintsViolated("index overflow".into()));
        }

        self.columns.insert(index, column);
        self.column_names.insert(index, column_name);

        Ok(())
    }
}

fn no_duplicates<T>(elements: &[T]) -> bool
where
    T: Hash + Eq + Clone,
{
    let mut uniq = HashSet::new();
    elements.iter().all(move |e| uniq.insert(e.clone()))
}

impl From<Table> for DictionaryImpl {
    fn from(value: Table) -> Self {
        let mut dict = Dictionary::new();
        let columns = value.columns;
        let column_names = value.column_names;

        for (k, v) in column_names.into_iter().zip(columns.into_iter()) {
            dict.insert_any(k.into(), v.into());
        }

        DictionaryImpl::String(dict)
    }
}

impl Table {
    pub const FORM_BYTE: DataForm = DataForm::Table;

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = PrettyTable::new();

        let name = self
            .column_names
            .iter()
            .map(|s| Cell::new(s.as_str()))
            .collect::<Vec<_>>();

        table.add_row(name.into());

        let columns = self
            .columns
            .iter()
            .map(|v| v.to_string())
            .map(|s| {
                s[1..s.len() - 1]
                    .split(", ")
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for j in 0..self.len() {
            let row = (0..self.columns.len())
                .map(|i| Cell::new(columns[i][j].as_str()))
                .collect::<Vec<_>>();

            table.add_row(row.into());
        }

        table.fmt(f)
    }
}

impl Constant for Table {
    fn data_form(&self) -> DataForm {
        Self::data_form()
    }

    fn data_type(&self) -> DataType {
        DataType::AnyDictionary
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl Serialize for Table {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        (self.data_type(), self.data_form()).serialize(buffer)?;

        buffer.put_i32(self.len() as i32);
        buffer.put_i32(self.columns.len() as i32);
        DolphinString::from(self.name.clone()).serialize(buffer)?;

        for name in self.column_names.iter() {
            DolphinString::from(name.clone()).serialize(buffer)?;
        }

        for column in self.columns.iter() {
            column.serialize(buffer)?;
        }

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        (self.data_type(), self.data_form()).serialize_le(buffer)?;

        buffer.put_i32_le(self.len() as i32);
        buffer.put_i32_le(self.columns.len() as i32);
        DolphinString::from(self.name.clone()).serialize_le(buffer)?;

        for name in self.column_names.iter() {
            DolphinString::from(name.clone()).serialize_le(buffer)?;
        }

        for column in self.columns.iter() {
            column.serialize_le(buffer)?;
        }

        Ok(0)
    }
}

impl Deserialize for Table {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32().await?)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let cols = reader.read_i32().await?;

        let table_name = {
            let mut name = DolphinString::default();
            name.deserialize(reader).await?;
            name.0.take().unwrap_or(String::new())
        };

        let mut column_names = Vec::new();
        let mut name = DolphinString::default();
        for _ in 0..cols {
            name.deserialize(reader).await?;
            column_names.push(name.0.take().unwrap_or(String::new()));
        }

        let mut columns = Vec::new();
        let mut symbol_base_dict: Option<HashMap<i32, Vec<Symbol>>> = Some(Default::default());

        for _ in 0..cols {
            let column = deserialize_vector(reader, &mut symbol_base_dict).await?;

            if column.len() != len {
                return Err(Error::ConstraintsViolated("mismatch column size".into()));
            }

            columns.push(column);
        }

        let mut builder = TableBuilder::new();
        builder
            .with_name(table_name)
            .with_contents(columns, column_names);
        *self = builder
            .build()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32_le().await?)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let cols = reader.read_i32_le().await?;

        let table_name = {
            let mut name = DolphinString::default();
            name.deserialize_le(reader).await?;
            name.0.take().unwrap_or(String::new())
        };

        let mut column_names = Vec::new();
        let mut name = DolphinString::default();
        for _ in 0..cols {
            name.deserialize_le(reader).await?;
            column_names.push(name.0.take().unwrap_or(String::new()));
        }

        let mut columns = Vec::new();
        let mut symbol_base_dict: Option<HashMap<i32, Vec<Symbol>>> = Some(Default::default());

        for _ in 0..cols {
            let column = deserialize_vector_le(reader, &mut symbol_base_dict).await?;

            if column.len() != len {
                return Err(Error::ConstraintsViolated("mismatch column size".into()));
            }

            columns.push(column);
        }

        let mut builder = TableBuilder::new();
        builder
            .with_name(table_name)
            .with_contents(columns, column_names);
        *self = builder
            .build()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

        Ok(())
    }
}
