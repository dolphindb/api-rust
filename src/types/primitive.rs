use super::{DataType, Scalar};
use crate::error::Error;

use std::any::type_name;
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

pub trait Primitive:
    Send + Sync + Clone + Default + PartialEq + Eq + PartialOrd + Ord + Hash + Scalar
{
    type RawType: Send + Sync + Clone;

    type RefType<'a>: Send + Copy;

    fn new(val: Self::RawType) -> Self;

    fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType;
}

type U8Vec = Vec<u8>;

macro_rules! integer_impl {
    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            #[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
            pub struct $struct_name(pub(crate) Option<$raw_type>);
        )*
    };
}

integer_impl!(
    ((), Void, Void),
    (bool, Bool, Bool),
    (i8, Char, Char),
    (i16, Short, Short),
    (i32, Int, Int),
    (i64, Long, Long)
);

macro_rules! float_impl {
    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            #[derive(Default, Clone, Copy, Debug)]
            pub struct $struct_name(pub(crate) Option<$raw_type>);
        )*
    };
}

float_impl!((f32, Float, Float), (f64, Double, Double));

macro_rules! eq_ord_hash_impl {
    ($raw_type:tt, $struct_name:ident, $enum_name:ident) => {
        impl PartialEq for $struct_name {
            #[inline]
            fn eq(&self, other: &$struct_name) -> bool {
                match (self.0, other.0) {
                    (None, None) => true,
                    (Some(a), Some(b)) => {
                        if a.is_nan() {
                            b.is_nan()
                        } else {
                            a == b
                        }
                    }
                    _ => false,
                }
            }
        }

        impl Eq for $struct_name {}

        impl PartialOrd for $struct_name {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }

            #[inline]
            fn lt(&self, other: &Self) -> bool {
                !self.ge(other)
            }

            #[inline]
            fn le(&self, other: &Self) -> bool {
                other.ge(self)
            }

            #[inline]
            fn gt(&self, other: &Self) -> bool {
                !other.ge(self)
            }

            #[inline]
            fn ge(&self, other: &Self) -> bool {
                // We consider all NaNs equal, and NaN is the largest possible
                // value. Thus if self is NaN we always return true. Otherwise
                // self >= other is correct. If other is also not NaN it is trivially
                // correct, and if it is we note that nothing can be greater or
                // equal to NaN except NaN itself, which we already handled earlier.
                match (self.0, other.0) {
                    (None, None) => true,
                    (None, Some(_)) => false,
                    (Some(_), None) => true,
                    (Some(a), Some(b)) => a.is_nan() | (a >= b),
                }
            }
        }

        impl Ord for $struct_name {
            fn cmp(&self, other: &Self) -> Ordering {
                if self < other {
                    Ordering::Less
                } else if self > other {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }

        impl Hash for $struct_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                let bits = if let Some(f) = self.0 {
                    if f.is_nan() {
                        0x7ff8000000000000u64
                    } else {
                        $raw_type::to_bits(f) as u64
                    }
                } else {
                    0
                };

                bits.hash(state)
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            eq_ord_hash_impl!($raw_type, $struct_name, $enum_name);
        )*
    };
}

eq_ord_hash_impl!((f32, Float, Float), (f64, Double, Double));

macro_rules! literal_impl {
    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            #[derive(Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
            pub struct $struct_name(pub(crate) Option<$raw_type>);
        )*
    };
}

literal_impl!(
    (String, Symbol, Symbol),
    (String, DolphinString, String),
    (U8Vec, Blob, Blob)
);

macro_rules! common_impl {
    ($raw_type:tt, $struct_name:ident, $enum_name:ident) => {
        impl $struct_name {
            pub const DATA_BYTE: DataType = DataType::$enum_name;

            pub fn new(val: $raw_type) -> Self {
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
            common_impl!($raw_type, $struct_name, $enum_name);
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

macro_rules! as_mut_impl {
    ($raw_type:tt, $struct_name:ident, $enum_name:ident) => {
        impl AsMut<Option<$raw_type>> for $struct_name {
            fn as_mut(&mut self) -> &mut Option<$raw_type> {
                &mut self.0
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            as_mut_impl!($raw_type, $struct_name, $enum_name);
        )*
    };
}

macro_rules! from_raw_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<$raw_type> for $struct_name {
            fn from(value: $raw_type) -> Self {
                Self::new(value.into())
            }
        }

        impl From<Option<$raw_type>> for $struct_name {
            fn from(value: Option<$raw_type>) -> Self {
                match value {
                    Some(val) => Self::new(val.into()),
                    None => Self::default(),
                }
            }
        }

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

macro_rules! primitive_impl {
    ((), Void) => {
        impl Primitive for Void {
            type RawType = ();

            type RefType<'a> = ();

            fn new(val: Self::RawType) -> Self {
                Self::new(val)
            }

            fn to_owned(_ref_data: Self::RefType<'_>) -> Self::RawType {
            }
        }
    };

    (String, $struct_name:ident) => {
        impl Primitive for $struct_name {
            type RawType = String;

            type RefType<'a> = &'a str;

            fn new(val: Self::RawType) -> Self {
                Self::new(val)
            }

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data.to_string()
            }
        }
    };

    ($raw_type:tt, Blob) => {
        impl Primitive for Blob {
            type RawType = $raw_type;

            type RefType<'a> = &'a $raw_type;

            fn new(val: Self::RawType) -> Self {
                Self::new(val)
            }

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data.clone()
            }
        }
    };

    ($raw_type:tt, $struct_name:ident) => {
        impl Primitive for $struct_name {
            type RawType = $raw_type;

            type RefType<'a> = $raw_type;

            fn new(val: Self::RawType) -> Self {
                Self::new(val)
            }

            fn to_owned(ref_data: Self::RefType<'_>) -> Self::RawType {
                ref_data
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $enum_name:ident)), *) => {
        $(
            primitive_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! for_all_types {
    ($macro:tt) => {
        $macro!(
            ((), Void, Void),
            (bool, Bool, Bool),
            (i8, Char, Char),
            (i16, Short, Short),
            (i32, Int, Int),
            (i64, Long, Long),
            (f32, Float, Float),
            (f64, Double, Double),
            (String, Symbol, Symbol),
            (String, DolphinString, String),
            (U8Vec, Blob, Blob)
        );
    };
}

for_all_types!(common_impl);

for_all_types!(as_ref_impl);

for_all_types!(as_mut_impl);

for_all_types!(from_raw_impl);

for_all_types!(primitive_impl);
