use std::{
    any::type_name,
    fmt::{self, Display},
};

use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};

use super::{
    decimal::*, primitive::*, temporal::*, Constant, DataForm, DataType, Vector, VectorImpl,
};

use tokio::io::AsyncBufReadExt;

#[derive(Debug, Clone)]
pub struct Pair<S>(Vector<S>);

impl<S> Pair<S> {
    pub const FORM_BYTE: u8 = 1;

    pub fn new(pair: (S, S)) -> Self {
        pair.into()
    }

    pub fn first(&self) -> &S {
        self.0.first().unwrap()
    }

    pub fn second(&self) -> &S {
        self.0.last().unwrap()
    }

    pub fn first_mut(&mut self) -> &mut S {
        self.0.first_mut().unwrap()
    }

    pub fn second_mut(&mut self) -> &mut S {
        self.0.last_mut().unwrap()
    }

    pub(crate) fn into_inner(self) -> Vector<S> {
        self.0
    }

    pub(crate) fn from_raw(val: Vector<S>) -> Self {
        Self(val)
    }
}

impl<S> From<(S, S)> for Pair<S> {
    fn from(value: (S, S)) -> Self {
        let mut v = Vector::new();
        v.push(value.0);
        v.push(value.1);
        Self(v)
    }
}

impl<S: Default> Default for Pair<S> {
    fn default() -> Self {
        (S::default(), S::default()).into()
    }
}

impl<S: Display> Display for Pair<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.first(), self.second())
    }
}

impl<S: PartialEq> PartialEq for Pair<S> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<S: Eq> Eq for Pair<S> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairImpl {
    Bool(Pair<Bool>),
    Char(Pair<Char>),
    Short(Pair<Short>),
    Int(Pair<Int>),
    Long(Pair<Long>),

    Date(Pair<Date>),
    Month(Pair<Month>),
    Time(Pair<Time>),
    Minute(Pair<Minute>),
    Second(Pair<Second>),
    DateTime(Pair<DateTime>),
    Timestamp(Pair<Timestamp>),
    NanoTime(Pair<NanoTime>),
    NanoTimestamp(Pair<NanoTimestamp>),

    Float(Pair<Float>),
    Double(Pair<Double>),

    Symbol(Pair<Symbol>),
    String(Pair<DolphinString>),

    DateHour(Pair<DateHour>),

    Decimal32(Pair<Decimal32>),
    Decimal64(Pair<Decimal64>),
    Decimal128(Pair<Decimal128>),
}

impl Constant for PairImpl {
    fn data_form(&self) -> DataForm {
        Self::data_form()
    }

    fn data_type(&self) -> DataType {
        self.data_type()
    }

    fn len(&self) -> usize {
        2
    }
}

macro_rules! for_all_pairs {
    ($macro:tt) => {
        $macro!(
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
            (Double, Double)
        );
    };
}

impl PairImpl {
    pub const FORM_BYTE: DataForm = DataForm::Pair;

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl PairImpl {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    $(
                        $struct_name::DATA_BYTE => Some(Self::$enum_name(Pair::default())),
                    )*
                    _ => None,
                }
            }
        }
    };
}

macro_rules! dispatch_from {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl From<PairImpl> for VectorImpl {
            fn from(value: PairImpl) -> Self {
                match value {
                    $(
                        PairImpl::$enum_name(val) => val.into(),
                    )*
                }
            }
        }

        impl TryFrom<VectorImpl> for PairImpl {
            type Error = Error;

            fn try_from(value: VectorImpl) -> Result<Self, Self::Error> {
                    match value {
                    $(
                        VectorImpl::$enum_name(val) => Ok(Self::$enum_name(val.into())),
                    )*
                    _ => Err(Error::InvalidConvert {
                        from: type_name::<VectorImpl>().to_string(),
                        to: type_name::<Self>().to_string(),
                    }),
                }
            }
        }
    };
}

macro_rules! from_impl {
    (DolphinString, DolphinString) => {
        from_impl!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<Vector<$struct_name>> for Pair<$struct_name> {
            fn from(value: Vector<$struct_name>) -> Self {
                Self::from_raw(value)
            }
        }

        impl From<Pair<$struct_name>> for Vector<$struct_name> {
            fn from(value: Pair<$struct_name>) -> Self {
                value.into_inner()
            }
        }

        impl From<Pair<$struct_name>> for PairImpl {
            fn from(value: Pair<$struct_name>) -> Self {
                Self::$enum_name(value)
            }
        }

        impl From<Pair<$struct_name>> for VectorImpl {
            fn from(value: Pair<$struct_name>) -> Self {
                Self::$enum_name(value.into())
            }
        }
    };

    ($(($raw_type:tt, $enum_name:ident)), *) => {
        $(
            from_impl!($enum_name, $enum_name);
        )*
    };
}

macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl PairImpl {
            pub fn data_type(&self) -> DataType {
                match self {
                    $(
                        PairImpl::$enum_name(_) => $struct_name::data_type(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for PairImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        PairImpl::$enum_name(v) => write!(f, "{}", v),
                    )*
                }
            }
        }
    };
}

for_all_pairs!(dispatch_reflect);

for_all_pairs!(dispatch_from);

for_all_pairs!(from_impl);

for_all_pairs!(dispatch_data_type);

for_all_pairs!(dispatch_display);

impl Serialize for PairImpl {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let v: VectorImpl = self.clone().into();
        (v.data_type(), self.data_form()).serialize(buffer)?;

        buffer.put_i32(self.len() as i32);
        buffer.put_i32(1);

        v.serialize_data(buffer)?;
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let v: VectorImpl = self.clone().into();
        (v.data_type(), self.data_form()).serialize_le(buffer)?;

        buffer.put_i32_le(self.len() as i32);
        buffer.put_i32_le(1);

        v.serialize_data_le(buffer)?;
        Ok(0)
    }
}

impl Deserialize for PairImpl {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut v: VectorImpl = self.clone().into();
        v.deserialize(reader).await?;

        *self = v.try_into()?;

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut v: VectorImpl = self.clone().into();
        v.deserialize_le(reader).await?;

        *self = v.try_into()?;

        Ok(())
    }
}
