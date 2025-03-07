mod decimal;
mod deserialize;
mod serialize;
mod temporal;

use super::{
    decimal::*, primitive::*, temporal::*, Any, Constant, ConstantImpl, DataForm, DataType, Vector,
    VectorImpl,
};

use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};

use std::{
    fmt::{self, Debug, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

use paste::paste;

pub use decimal::DecimalInterface;

pub trait Scalar: Send + Sync + Clone + Debug + Default + PartialEq + PartialOrd + Hash {
    fn data_type() -> DataType;

    fn is_null(&self) -> bool;
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum ScalarImpl {
    Void(Void),
    Bool(Bool),
    Char(Char),
    Short(Short),
    Int(Int),
    Long(Long),

    Date(Date),
    Month(Month),
    Time(Time),
    Minute(Minute),
    Second(Second),
    DateTime(DateTime),
    Timestamp(Timestamp),
    NanoTime(NanoTime),
    NanoTimestamp(NanoTimestamp),

    Float(Float),
    Double(Double),

    Symbol(Symbol),
    String(DolphinString),

    DateHour(DateHour),

    Blob(Blob),

    Decimal32(Decimal32),
    Decimal64(Decimal64),
    Decimal128(Decimal128),
}

impl Default for ScalarImpl {
    fn default() -> Self {
        Self::Void(Void::default())
    }
}

macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl ScalarImpl {
            pub fn data_type(&self) -> DataType {
                match self {
                    $(
                        ScalarImpl::$enum_name(_) => $struct_name::data_type(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Serialize for ScalarImpl {
            fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                (self.data_type(), self.data_form()).serialize(buffer)?;

                match self {
                    $(
                        Self::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                (self.data_type(), self.data_form()).serialize_le(buffer)?;

                match self {
                    $(
                        Self::$enum_name(s) => s.serialize_le(buffer),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_deserialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Deserialize for ScalarImpl {
            async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize(reader).await,
                    )*
                }
            }

            async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for ScalarImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        ScalarImpl::$enum_name(s) => write!(f, "{}", s),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl ScalarImpl {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    $(
                        $struct_name::DATA_BYTE => Some(Self::$enum_name($struct_name::default())),
                    )*
                    _ => None,
                }
            }
        }
    };
}

macro_rules! dispatch_is_null {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl ScalarImpl {
            pub(crate) fn is_null(&self) -> bool {
                match self {
                    $(
                        ScalarImpl::$enum_name(s) => s.is_null(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_push_vector {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub fn push_scalar(&mut self, value: ScalarImpl) {
                match self {
                    $(
                        VectorImpl::$enum_name(data) => data.push(match value {
                            ScalarImpl::$enum_name(v) => v,
                            _ => unreachable!(),
                        }),
                    )*
                    _ => unreachable!(),
                }
            }
        }
    };
}

macro_rules! dispatch_break_up {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub fn break_up(self) -> Vector<Any> {
                match self {
                    $(
                        VectorImpl::$enum_name(data) => {
                            let mut res = Vector::new();
                            for v in data {
                                let s: ScalarImpl = v.into();
                                res.push(s.into());
                            }
                            res
                        }
                    )*
                    VectorImpl::Any(data) => data,
                    VectorImpl::ArrayVector(_v) => Vector::new(), // Unsupported
                }
            }
        }
    };
}

macro_rules! dispatch_get {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub fn get(&self, index: usize) -> Option<ConstantImpl> {
                match self {
                    $(
                        VectorImpl::$enum_name(val) => Some(val[index].to_owned().into()),
                    )*
                    VectorImpl::ArrayVector(_v) => None, // Unsupported now
                    VectorImpl::Any(v) => Some(v[index].0.clone())
                }
            }
        }
    };
}

macro_rules! dispatch_as {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        paste! {
            impl ScalarImpl {
                $(
                    pub fn [<as_ $enum_name:snake>](&self) -> Result<&$struct_name, Error> {
                        match self {
                            ScalarImpl::$enum_name(v) => Ok(v),
                            _ => Err(Error::InvalidConvert {
                                from: self.data_type().to_string(),
                                to: $struct_name::data_type().to_string(),
                            }),
                        }
                    }

                    pub fn [<as_mut_ $enum_name:snake>](&mut self) -> Result<&mut $struct_name, Error> {
                        match self {
                            ScalarImpl::$enum_name(v) => Ok(v),
                            _ => Err(Error::InvalidConvert {
                                from: self.data_type().to_string(),
                                to: $struct_name::data_type().to_string(),
                            }),
                        }
                    }
                )*
            }
        }
    };
}

macro_rules! for_all_scalars {
    ($macro:tt) => {
        $macro!(
            (Void, Void),
            (Bool, Bool),
            (Date, Date),
            (Month, Month),
            (Time, Time),
            (Minute, Minute),
            (Second, Second),
            (DateTime, DateTime),
            (Timestamp, Timestamp),
            (NanoTime, NanoTime),
            (NanoTimestamp, NanoTimestamp),
            (Symbol, Symbol),
            (String, DolphinString),
            (DateHour, DateHour),
            (Decimal32, Decimal32),
            (Decimal64, Decimal64),
            (Decimal128, Decimal128),
            (Char, Char),
            (Short, Short),
            (Int, Int),
            (Long, Long),
            (Float, Float),
            (Double, Double),
            (Blob, Blob)
        );
    };
}

for_all_scalars!(dispatch_data_type);

for_all_scalars!(dispatch_serialize);

for_all_scalars!(dispatch_deserialize);

for_all_scalars!(dispatch_display);

for_all_scalars!(dispatch_reflect);

for_all_scalars!(dispatch_is_null);

for_all_scalars!(dispatch_push_vector);

for_all_scalars!(dispatch_break_up);

for_all_scalars!(dispatch_get);

for_all_scalars!(dispatch_as);

impl ScalarImpl {
    pub const FORM_BYTE: DataForm = DataForm::Scalar;

    pub const fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

impl Constant for ScalarImpl {
    fn data_form(&self) -> DataForm {
        Self::data_form()
    }

    fn data_type(&self) -> DataType {
        self.data_type()
    }

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }
}
