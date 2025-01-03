use super::DataType;
use crate::error::Error;

use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use std::any::type_name;

macro_rules! temporal_impl {
    ($raw_type:tt, $struct_name:ident, $enum_name:ident) => {
        #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        pub struct $struct_name(pub(crate) Option<$raw_type>);

        impl $struct_name {
            pub const DATA_BYTE: DataType = DataType::$enum_name;

            pub(crate) fn new(val: $raw_type) -> Self {
                Self(Some(val))
            }

            pub const fn data_type() -> DataType {
                Self::DATA_BYTE
            }

            pub const fn is_null(&self) -> bool {
                self.0.is_none()
            }

            pub fn into_inner(self) -> Option<$raw_type> {
                self.0
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            temporal_impl!($raw_type, $struct_name, $enum_name);
        )*
    };
}

macro_rules! as_ref_impl {
    ($raw_type:tt, $struct_name:ident, $enum_name:ident) => {
        impl AsRef<Option<$raw_type>> for $struct_name {
            fn as_ref(&self) -> &Option<$raw_type> {
                &self.0
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            as_ref_impl!($raw_type, $struct_name, $enum_name);
        )*
    };
}

macro_rules! from_raw_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl TryFrom<$struct_name> for $raw_type {
            type Error = Error;

            fn try_from(value: $struct_name) -> Result<Self, Self::Error> {
                match value.into_inner() {
                    Some(value) => Ok(value),
                    _ => Err(Error::InvalidConvert {
                        from: "null".into(),
                        to: type_name::<$raw_type>().to_string(),
                    }),
                }
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            from_raw_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! for_all_types {
    ($macro:tt) => {
        $macro!(
            (NaiveDate, Date, Date),
            (NaiveDate, Month, Month),
            (NaiveTime, Time, Time),
            (NaiveTime, Minute, Minute),
            (NaiveTime, Second, Second),
            (NaiveDateTime, DateTime, DateTime),
            (NaiveDateTime, Timestamp, Timestamp),
            (NaiveTime, NanoTime, NanoTime),
            (NaiveDateTime, NanoTimestamp, NanoTimestamp),
            (NaiveDateTime, DateHour, DateHour)
        );
    };
}

for_all_types!(temporal_impl);

for_all_types!(as_ref_impl);

for_all_types!(from_raw_impl);
