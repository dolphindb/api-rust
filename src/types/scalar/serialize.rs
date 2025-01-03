use crate::{
    error::{Error, Result},
    Serialize,
};
use bytes::BufMut;

use byteorder::{WriteBytesExt, BE, LE};

use crate::types::{any::Any, decimal::*, primitive::*, temporal::*};

impl Serialize for Void {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let mut writer = buffer.writer();
        writer.write_i8(0).unwrap();
        Ok(0)
    }
}

impl Serialize for Bool {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        buffer.put_i8(self.as_ref().map(|b| b as i8).unwrap_or(i8::MIN));
        Ok(0)
    }
}

impl Serialize for Char {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        buffer.put_i8(self.as_ref().unwrap_or(i8::MIN));
        Ok(0)
    }
}

macro_rules! serialize_literal {
    ($struct_name:ident) => {
        impl Serialize for $struct_name {
            fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: BufMut,
            {
                buffer.put(self.as_ref().as_ref().map(|s| s.as_str()).unwrap_or("").as_bytes());
                buffer.put_u8(b'\0');
                Ok(0)
            }
        }
    };

    ($($struct_name:ident), *) => {
        $(
            serialize_literal!($struct_name);
        )*
    };
}

serialize_literal!(DolphinString, Symbol);

macro_rules! serialize_primitive {
    ($raw_type:tt, $write_func:ident, $func_name:ident, $endian:tt) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize>
        where
            B: bytes::BufMut,
        {
            let mut writer = buffer.writer();
            writer
                .$write_func::<$endian>(self.into_inner().unwrap_or($raw_type::MIN.into()).into())
                .unwrap();
            Ok(0)
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $write_func:ident)), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_primitive!($raw_type, $write_func, serialize, BE);
                serialize_primitive!($raw_type, $write_func, serialize_le, LE);
            }
        )*
    };
}

serialize_primitive!(
    (i16, Short, write_i16),
    (i32, Int, write_i32),
    (i64, Long, write_i64),
    (f32, Float, write_f32),
    (f64, Double, write_f64)
);

macro_rules! serialize_i32_temporal {
    ($func_name:ident, $offset:expr) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize>
        where
            B: BufMut,
        {
            Int::from(self.elapsed().map(|i| i as i32 + $offset)).$func_name(buffer)
        }
    };

    ($(($struct_name:ident, $offset:expr)), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_i32_temporal!(serialize, $offset);
                serialize_i32_temporal!(serialize_le, $offset);
            }
        )*
    }
}

macro_rules! serialize_i64_temporal {
    ($func_name:ident) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize>
        where
            B: BufMut,
        {
            Long::from(self.elapsed().map(|i| i as i64)).$func_name(buffer)
        }
    };

    ($($struct_name:ident), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_i64_temporal!(serialize);
                serialize_i64_temporal!(serialize_le);
            }
        )*
    }
}

serialize_i32_temporal!(
    (Date, 0),
    (Month, 23640),
    (Time, 0),
    (Minute, 0),
    (Second, 0),
    (DateTime, 0),
    (DateHour, 0)
);
serialize_i64_temporal!(Timestamp, NanoTime, NanoTimestamp);

macro_rules! serialize_decimal {
    ($raw_type:tt, $write_func:ident, $func_name:ident, $endian:tt) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize>
        where
            B: bytes::BufMut,
        {
            let mut writer = buffer.writer();

            writer
                .write_i32::<$endian>(self.scale().unwrap_or(0) as i32)
                .unwrap();
            writer
                .$write_func::<$endian>(self.mantissa().unwrap_or($raw_type::MIN))
                .unwrap();
            Ok(0)
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $write_func:ident)), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_decimal!($raw_type, $write_func, serialize, BE);
                serialize_decimal!($raw_type, $write_func, serialize_le, LE);
            }
        )*
    };
}

serialize_decimal!(
    (i32, Decimal32, write_i32),
    (i64, Decimal64, write_i64),
    (i128, Decimal128, write_i128)
);

impl Serialize for Any {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        self.0.serialize(buffer)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        self.0.serialize_le(buffer)
    }
}

impl Serialize for Blob {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        let phantom = Vec::with_capacity(0);
        let data = self.0.as_ref().unwrap_or(&phantom);
        let len =
            u32::try_from(data.len()).map_err(|e| Error::ConstraintsViolated(e.to_string()))?;

        buffer.put_u32(len);
        buffer.put(&data[..]);

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        let phantom = Vec::with_capacity(0);
        let data = self.0.as_ref().unwrap_or(&phantom);
        let len =
            u32::try_from(data.len()).map_err(|e| Error::ConstraintsViolated(e.to_string()))?;

        buffer.put_u32_le(len);
        buffer.put(&data[..]);

        Ok(0)
    }
}
