use byteorder::{WriteBytesExt, BE, LE};
use bytes::BufMut;

use super::{
    Bool, Char, Date, DateHour, DateTime, DolphinString, Double, Float, Int, Long, Minute, Month,
    NanoTime, NanoTimeStamp, Second, Short, Time, TimeStamp,
};
use crate::Serialize;

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
        buffer.put_i8(self.0);
        Ok(0)
    }
}

impl Serialize for Char {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        buffer.put_i8(self.0);
        Ok(0)
    }
}

impl Serialize for DolphinString {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: BufMut,
    {
        buffer.put(self.0.as_bytes());
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
                .$write_func::<$endian>(self.0)
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
    (i64, Long, write_i64)
);

macro_rules! serialize_primitive2 {
    ($raw_type:tt, $write_func:ident, $func_name:ident, $endian:tt) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
        where
            B: bytes::BufMut,
        {
            let mut writer = buffer.writer();
            writer
                .$write_func::<$endian>(self.0.0)
                .unwrap();
            Ok(0)
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $write_func:ident)), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_primitive2!($raw_type, $write_func, serialize, BE);
                serialize_primitive2!($raw_type, $write_func, serialize_le, LE);
            }
        )*
    };
}

serialize_primitive2!((f32, Float, write_f32), (f64, Double, write_f64));

macro_rules! serialize_i32_temporal {
    ($func_name:ident, $struct_name:ident) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
        where
            B: BufMut,
        {
            Int::new(self.0).$func_name(buffer)
        }
    };

    ($($struct_name:ident), *) => {
        $(
            impl Serialize for $struct_name {
                serialize_i32_temporal!(serialize, $struct_name);
                serialize_i32_temporal!(serialize_le, $struct_name);
            }
        )*
    }
}
serialize_i32_temporal!(Date, Month, Time, Minute, Second, DateTime, DateHour);

macro_rules! serialize_i64_temporal {
    ($func_name:ident) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize, ()>
        where
            B: BufMut,
        {
            Long::new(self.elapsed().map(|i| i as i64).unwrap()).$func_name(buffer)
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
serialize_i64_temporal!(TimeStamp, NanoTime, NanoTimeStamp);
