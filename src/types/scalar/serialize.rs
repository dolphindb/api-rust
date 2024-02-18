use crate::Serialize;
use bytes::BufMut;

use byteorder::{WriteBytesExt, BE, LE};

use super::scalar::*;

impl Serialize for () {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        let mut writer = buffer.writer();
        writer.write_i8(0).unwrap();
        Ok(0)
    }
}

impl Serialize for Bool {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        buffer.put_i8(self.as_ref().map(|b| *b as i8).unwrap_or(i8::MIN));
        Ok(0)
    }
}

impl Serialize for Char {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        buffer.put_i8(self.as_ref().map(|c| *c as i8).unwrap_or(i8::MIN));
        Ok(0)
    }
}

impl Serialize for DolphinString {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: BufMut,
    {
        buffer.put(self.as_ref().map(|s| s.as_str()).unwrap_or("").as_bytes());
        buffer.put_u8(b'\0');
        Ok(0)
    }
}

macro_rules! serialize_primitive {
    ($raw_type:tt, $write_func:ident, $func_name:ident, $endian:tt) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
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
    ($func_name:ident) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
        where
            B: BufMut,
        {
            Int::new(self.elapsed().map(|i| i as i32)).$func_name(buffer)
        }
    };

    ($($struct_name:ident), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_i32_temporal!(serialize);
                serialize_i32_temporal!(serialize_le);
            }
        )*
    }
}

macro_rules! serialize_i64_temporal {
    ($func_name:ident) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
        where
            B: BufMut,
        {
            Long::new(self.elapsed().map(|i| i as i64)).$func_name(buffer)
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

serialize_i32_temporal!(Date, Month, Time, Minute, Second, DateTime, DateHour);
serialize_i64_temporal!(TimeStamp, NanoTime, NanoTimeStamp);

macro_rules! serialize_decimal {
    ($raw_type:tt, $write_func:ident, $func_name:ident, $endian:tt) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
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
