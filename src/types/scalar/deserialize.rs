use crate::Deserialize;
use std::io::{Error, ErrorKind, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::{
    Bool, Char, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long, Minute, Month,
    NanoTime, NanoTimeStamp, Second, Short, Time, TimeStamp,
};

// implement deserialize for some scalar types no need to consider endianness
impl Deserialize for () {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        reader.read_i8().await?;
        Ok(())
    }
}

impl Deserialize for Bool {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.set_raw(reader.read_i8().await?);
        Ok(())
    }
}

impl Deserialize for Char {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.set_raw(reader.read_i8().await?);
        Ok(())
    }
}

impl Deserialize for DolphinString {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        // read_until would use Vec::extend_from_slice to increase buffer size, so no need to preallocate memory for buf.
        let mut buf = Vec::new();
        let n = reader.read_until(b'\0', &mut buf).await?;

        if n == 0 {
            return Err(Error::from(ErrorKind::InvalidData));
        }

        if *buf.last().unwrap() != b'\0' {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }
        buf.pop();

        if buf.is_empty() {
            self.set_null();
        } else {
            self.set_raw(
                String::from_utf8(buf)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
            );
        }

        Ok(())
    }
}

// implement deserialize for integeral scalar types
macro_rules! deserialize_primitive {
    ($raw_type:tt, $read_func:ident, $func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            self.set_raw(reader.$read_func().await?);
            Ok(())
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $read_func:ident, $read_func_le:ident)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_primitive!($raw_type, $read_func, deserialize);
                deserialize_primitive!($raw_type, $read_func_le, deserialize_le);
            }
        )*
    };
}

deserialize_primitive!(
    (i16, Short, read_i16, read_i16_le),
    (i32, Int, read_i32, read_i32_le),
    (i64, Long, read_i64, read_i64_le),
    (f32, Float, read_f32, read_f32_le),
    (f64, Double, read_f64, read_f64_le)
);

// implement deserialize for 32 bit temporal saclar types
macro_rules! deserialize_i32_temporal {
    ($func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut temporal_i32 = Int::new(0);
            temporal_i32.$func_name(reader).await?;
            self.set_raw(temporal_i32.get_raw());
            Ok(())
        }
    };

    ($(($struct_name:ident)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_i32_temporal!(deserialize);
                deserialize_i32_temporal!(deserialize_le);
            }
        )*
    };
}

deserialize_i32_temporal!(
    (Date),
    (Month),
    (Time),
    (Minute),
    (Second),
    (DateTime),
    (DateHour)
);

// implement deserialize for 64 bit temporal saclar types
macro_rules! deserialize_i64_temporal {
    ($func_name:ident, $elapsed_type:tt,$struct_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut temporal_i64 = Long::new(0);
            temporal_i64.$func_name(reader).await?;
            self.set_raw(temporal_i64.get_raw());
            Ok(())
        }
    };

    ($(($struct_name:ident, $elapsed_type:tt)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_i64_temporal!(deserialize, $elapsed_type, $struct_name);
                deserialize_i64_temporal!(deserialize_le, $elapsed_type,$struct_name);
            }
        )*
    };
}

deserialize_i64_temporal!((TimeStamp, i64), (NanoTime, u64), (NanoTimeStamp, i64));
