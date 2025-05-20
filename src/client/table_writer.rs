use std::vec;

use tokio::{runtime::Handle, task::block_in_place};

use crate::{error::Error, types::*};

use super::Client;

/// This is a simple buffer for tableInsert
pub struct TableWriter {
    client: Client,
    table_name: String,
    script: String,
    columns: Vec<VectorImpl>,
    column_types: Vec<DataType>,
    column_names: Vec<String>,
    buffer: Vec<VectorImpl>,
    size: u32,
    batch_size: u32,
}

/// TableWriter is a simple buffer for tableInsert.
///
/// Partitioned tables are not supported.
impl TableWriter {
    /// Creates a `TableWriter`.
    ///
    /// This function will try to get the table's schema through client,
    /// so the caller needs to make sure table is created before calling this function.
    ///
    /// # Examples
    ///
    /// ```
    /// use dolphindb::client::{ClientBuilder, TableWriter};
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut builder = ClientBuilder::new("127.0.0.1:8848");
    ///     builder.with_auth(("admin", "123456"));
    ///     let mut client = builder.connect().await.unwrap();
    ///     let mut table = TableWriter::new(client, "test_table", 512);
    /// }
    /// ```
    pub async fn new(mut client: Client, table_name: &str, batch_size: u32) -> Self {
        if batch_size == 0 {
            panic!("TableWriter: batch_size must be positive.");
        }
        let mut columns = vec![];
        let mut column_types = vec![];
        let mut column_names: Vec<String> = vec![];
        let schema = client
            .run_script(format!("schema({}).colDefs.typeInt", table_name).as_str())
            .await;
        if schema.is_err() {
            panic!("Failed to get schema for table {}", table_name);
        }
        if let VectorImpl::Int(columns_ddb) = schema.unwrap().unwrap().as_vector().unwrap() {
            for i in columns_ddb.iter() {
                let data_type = DataType::try_from(i.0.unwrap() as u8).unwrap();
                column_types.push(data_type);
                let vec = match data_type {
                    DataType::Bool => BoolVector::new().into(),
                    DataType::Char => CharVector::new().into(),
                    DataType::Short => ShortVector::new().into(),
                    DataType::Int
                    | DataType::Date
                    | DataType::Month
                    | DataType::Time
                    | DataType::Minute
                    | DataType::Second
                    | DataType::DateTime
                    | DataType::DateHour => IntVector::new().into(),
                    DataType::Long
                    | DataType::Timestamp
                    | DataType::NanoTime
                    | DataType::NanoTimestamp => LongVector::new().into(),
                    DataType::Float => FloatVector::new().into(),
                    DataType::Double => DoubleVector::new().into(),
                    DataType::String | DataType::Symbol => StringVector::new().into(),
                    DataType::Decimal32 => Decimal32Vector::new().into(),
                    DataType::Decimal64 => Decimal64Vector::new().into(),
                    DataType::Decimal128 => Decimal128Vector::new().into(),
                    DataType::CharArray => CharArrayVector::new().into(),
                    DataType::ShortArray => ShortArrayVector::new().into(),
                    DataType::IntArray => IntArrayVector::new().into(),
                    DataType::LongArray => LongArrayVector::new().into(),
                    DataType::FloatArray => FloatArrayVector::new().into(),
                    DataType::DoubleArray => DoubleArrayVector::new().into(),
                    _ => unimplemented!(),
                };
                columns.push(vec);
            }
        }
        if let VectorImpl::String(column_names_ddb) = client
            .run_script(format!("schema({}).colDefs.name", table_name).as_str())
            .await
            .unwrap()
            .unwrap()
            .as_vector()
            .unwrap()
        {
            column_names = column_names_ddb
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>();
        }
        let buffer = columns.clone();
        Self {
            client,
            table_name: table_name.to_string(),
            script: format!("tableInsert{{'{}'}}", table_name),
            columns,
            column_types,
            column_names,
            buffer,
            size: 0,
            batch_size,
        }
    }
    /// Append one row to the TableWriter's buffer.
    ///
    /// When buffer is full, this function will run tableInsert and return the result of the script.
    /// When buffer is not full, None is returned.
    ///
    /// This interface is NOT thread-safe.
    pub async fn append_row(
        &mut self,
        row: &mut Vec<PrimitiveType>,
    ) -> Result<Option<ConstantImpl>, Error> {
        if self.buffer.len() != row.len() {
            panic!(
                "Table {} has {} columns, but {} provided.",
                self.table_name,
                self.buffer.len(),
                row.len()
            );
        }
        let mut res: Result<(), Error> = Ok(());
        for i in (0..self.buffer.len()).rev() {
            let data = row.pop().unwrap();
            match &data {
                PrimitiveType::NaiveDate(d) => match self.column_types[i] {
                    DataType::Date => {
                        _ = self.buffer[i].push_primitive_type(Date::new(*d).ddb_rep().into())
                    }
                    DataType::Month => {
                        _ = self.buffer[i].push_primitive_type(Month::new(*d).ddb_rep().into())
                    }
                    t => {
                        res = Err(Error::InvalidConvert {
                            from: "NaiveDate".to_string(),
                            to: t.to_string(),
                        })
                    }
                },
                PrimitiveType::NaiveTime(t) => match self.column_types[i] {
                    DataType::Time => {
                        _ = self.buffer[i].push_primitive_type(Time::new(*t).ddb_rep().into())
                    }
                    DataType::Minute => {
                        _ = self.buffer[i].push_primitive_type(Minute::new(*t).ddb_rep().into())
                    }
                    DataType::Second => {
                        _ = self.buffer[i].push_primitive_type(Second::new(*t).ddb_rep().into())
                    }
                    DataType::NanoTime => {
                        _ = self.buffer[i].push_primitive_type(NanoTime::new(*t).ddb_rep().into())
                    }
                    t => {
                        res = Err(Error::InvalidConvert {
                            from: "NaiveTime".to_string(),
                            to: t.to_string(),
                        })
                    }
                },
                PrimitiveType::NaiveDateTime(dt) => match self.column_types[i] {
                    DataType::Timestamp => {
                        _ = self.buffer[i].push_primitive_type(Timestamp::new(*dt).ddb_rep().into())
                    }
                    DataType::NanoTimestamp => {
                        _ = self.buffer[i]
                            .push_primitive_type(NanoTimestamp::new(*dt).ddb_rep().into())
                    }
                    DataType::DateTime => {
                        _ = self.buffer[i].push_primitive_type(DateTime::new(*dt).ddb_rep().into())
                    }
                    DataType::DateHour => {
                        _ = self.buffer[i].push_primitive_type(DateHour::new(*dt).ddb_rep().into())
                    }
                    t => {
                        res = Err(Error::InvalidConvert {
                            from: "NaiveDateTime".to_string(),
                            to: t.to_string(),
                        })
                    }
                },
                _ => res = self.buffer[i].push_primitive_type(data),
            };
            if let Err(err) = res {
                panic!(
                    "Failed to insert into column `{}`: {}.",
                    self.column_names[i].clone(),
                    err.to_string()
                );
            }
        }
        self.size += 1;
        if self.size == self.batch_size {
            return self.flush().await;
        }
        Ok(None)
    }
    /// Manually flush the buffer.
    pub async fn flush(&mut self) -> Result<Option<ConstantImpl>, Error> {
        let mut builder = TableBuilder::new();
        let content = std::mem::take(&mut self.buffer);
        self.buffer = self.columns.clone();
        self.size = 0;
        builder.with_contents(content, self.column_names.clone());
        self.client
            .run_function(self.script.as_str(), &[builder.build().unwrap().into()])
            .await
    }
    /// Returns the number of rows in the buffer.
    pub fn size(&self) -> usize {
        self.buffer[0].len()
    }
}

impl Drop for TableWriter {
    fn drop(&mut self) {
        block_in_place(|| {
            Handle::current().block_on(async move {
                let _ = self.flush().await;
            });
        });
    }
}
