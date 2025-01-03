use crate::{
    error::{Error, Result},
    Deserialize,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::types::{any::Any, decimal::*, primitive::*, temporal::*};

impl Deserialize for Void {
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
            0 => self.0 = Some(false),
            i8::MIN => self.0 = None,
            _ => self.0 = Some(true),
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
            self.0 = None;
        } else {
            self.0 = Some(val as i8);
        }

        Ok(())
    }
}

macro_rules! deserialize_literal {
    ($struct_name:ident) => {
        impl Deserialize for $struct_name {
            async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                // read_until would use Vec::extend_from_slice to increase buffer size,
                // no need to preallocate memory for buf.
                let mut buf = Vec::new();
                let n = reader.read_until(b'\0', &mut buf).await?;

                if n == 0 {
                    return Err(Error::UnexpectedEof);
                }

                if *buf.last().unwrap() != b'\0' {
                    return Err(Error::UnexpectedEof);
                }

                buf.pop();

                if buf.is_empty() {
                    self.0 = None;
                } else {
                    self.0 = Some(String::from_utf8(buf)?);
                }

                Ok(())
            }
        }
    };

    ($(($struct_name:ident)), *) => {
        $(
            deserialize_literal!($struct_name);
        )*
    };
}

deserialize_literal!((DolphinString), (Symbol));

macro_rules! deserialize_primitive {
    ($raw_type:tt, $read_func:ident, $func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let val = reader.$read_func().await?;

            if val == $raw_type::MIN {
                self.0 = None;
            } else {
                self.0 = Some(val.into());
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
    ($func_name:ident, $elapsed_type:tt, $offset:expr) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut int = Int::default();
            int.$func_name(reader).await?;

            if let Some(elapsed) = int.as_ref().map(|v| $elapsed_type::try_from(v).ok()).flatten() {
                *self = Self::from_raw(elapsed - $offset)
                        .ok_or(Error::ConstraintsViolated(
                            "time elapsed out of bound".into(),
                        ))?;
            } else {
                self.0 = None;
            }

            Ok(())
        }
    };

    ($(($struct_name:ident, $elapsed_type:tt, $offset:expr)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_i32_temporal!(deserialize, $elapsed_type, $offset);
                deserialize_i32_temporal!(deserialize_le, $elapsed_type, $offset);
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

            if let Some(elapsed) = long.as_ref().map(|v| $elapsed_type::try_from(v).ok()).flatten() {
                *self = Self::from_raw(elapsed)
                        .ok_or(Error::ConstraintsViolated(
                            "time elapsed out of bound".into(),
                        ))?;
            } else {
                self.0 = None;
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
    (Date, i64, 0),
    (Month, i32, 23640),
    (Time, u32, 0),
    (Minute, u32, 0),
    (Second, u32, 0),
    (DateTime, i32, 0),
    (DateHour, i64, 0)
);

deserialize_i64_temporal!((Timestamp, i64), (NanoTime, u64), (NanoTimestamp, i64));

macro_rules! deserialize_decimal {
    ($raw_type:tt, $read_func:ident, $read_scale:ident, $func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let scale = reader.$read_scale().await?;
            let mantissa = reader.$read_func().await?;

            if mantissa != $raw_type::MIN {
                *self = Self::from_raw(mantissa, scale as u32).ok_or(Error::ConstraintsViolated("decimal scale overflow".into()))?;
            } else {
                self.0 = None;
            }

            Ok(())
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $read_func:ident, $read_func_le:ident)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_decimal!($raw_type, $read_func, read_i32, deserialize);
                deserialize_decimal!($raw_type, $read_func_le, read_i32_le, deserialize_le);
            }
        )*
    };
}

deserialize_decimal!(
    (i32, Decimal32, read_i32, read_i32_le),
    (i64, Decimal64, read_i64, read_i64_le),
    (i128, Decimal128, read_i128, read_i128_le)
);

impl Deserialize for Any {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        Box::pin(self.0.deserialize(reader)).await
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        Box::pin(self.0.deserialize_le(reader)).await
    }
}

impl Deserialize for Blob {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = reader.read_u32().await?;
        let mut data = vec![0u8; len as usize];
        reader.read_exact(&mut data).await?;

        if data.is_empty() {
            self.0 = None
        } else {
            self.0 = Some(data)
        }

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = reader.read_u32_le().await?;
        let mut data = vec![0u8; len as usize];
        reader.read_exact(&mut data).await?;

        if data.is_empty() {
            self.0 = None
        } else {
            self.0 = Some(data)
        }

        Ok(())
    }
}
