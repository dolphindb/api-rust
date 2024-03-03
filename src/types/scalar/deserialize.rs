use crate::Deserialize;
use std::io::{Error, ErrorKind, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::{
    Bool, Char, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long, Minute, Month,
    NanoTime, NanoTimeStamp, Second, Short, Time, TimeStamp,
};

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
        let val = reader.read_i8().await?;

        match val {
            0 => self.set(Some(false)),
            i8::MIN => self.set(None),
            _ => self.set(Some(true)),
        }

        Ok(())
    }
}

impl Deserialize for Char {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let val = reader.read_i8().await?;

        if val == i8::MIN {
            self.set(None);
        } else {
            self.set(Some(val as u8));
        }

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
            self.set(None);
        } else {
            self.set(Some(String::from_utf8(buf).map_err(|e| {
                Error::new(ErrorKind::InvalidData, e.to_string())
            })?));
        }

        Ok(())
    }
}

macro_rules! deserialize_primitive {
    ($raw_type:tt, $read_func:ident, $func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let val = reader.$read_func().await?;

            if val == $raw_type::MIN {
                self.set(None);
            } else {
                self.set(Some(val.into()));
            }

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

macro_rules! deserialize_i32_temporal {
    ($func_name:ident, $elapsed_type:tt) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut int = Int::default();
            int.$func_name(reader).await?;

            if let Some(elapsed) = int.as_ref().map(|v| $elapsed_type::try_from(*v).ok()).flatten() {
                *self = Self::from_raw(elapsed);
            } else {
                self.set(None);
            }

            Ok(())
        }
    };

    ($(($struct_name:ident, $elapsed_type:tt)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_i32_temporal!(deserialize, $elapsed_type);
                deserialize_i32_temporal!(deserialize_le, $elapsed_type);
            }
        )*
    };
}

macro_rules! deserialize_i64_temporal {
    ($func_name:ident, $elapsed_type:tt) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut long = Long::default();
            long.$func_name(reader).await?;

            if let Some(elapsed) = long.as_ref().map(|v| $elapsed_type::try_from(*v).ok()).flatten() {
                *self = Self::from_raw(elapsed);
            } else {
                self.set(None);
            }

            Ok(())
        }
    };

    ($(($struct_name:ident, $elapsed_type:tt)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_i64_temporal!(deserialize, $elapsed_type);
                deserialize_i64_temporal!(deserialize_le, $elapsed_type);
            }
        )*
    };
}

deserialize_i32_temporal!(
    (Date, i64),
    (Month, i32),
    (Time, u32),
    (Minute, u32),
    (Second, u32),
    (DateTime, i64),
    (DateHour, i64)
);

deserialize_i64_temporal!((TimeStamp, i64), (NanoTime, u64), (NanoTimeStamp, i64));
