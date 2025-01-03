use std::{
    any::type_name,
    borrow::Borrow,
    collections::{
        hash_set::{IntoIter, Iter},
        HashSet,
    },
    fmt::{self, Display},
    hash::Hash,
};
use tokio::io::AsyncBufReadExt;

use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};

use super::{
    decimal::*, deserialize_vector, deserialize_vector_le, primitive::*, temporal::*, Constant,
    DataForm, DataType, Scalar, Vector, VectorImpl,
};

/// DolphinDB's `Set` implemented base on `std::collections::HashSet` but
/// bounded on DolphinDB's `Scalar` type. use `HashSet` for other types instead.
#[derive(Default, Debug, Clone)]
pub struct Set<T> {
    data: HashSet<T>,
}

impl<T> Set<T> {
    /// Creates an empty `Set`.
    pub fn new() -> Self {
        Self {
            data: HashSet::new(),
        }
    }

    /// Creates an empty `Set` with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: HashSet::with_capacity(capacity),
        }
    }

    /// Returns the number of elements the set can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// An iterator visiting all elements in arbitrary order.
    /// The iterator element type is `&'a T`.
    pub fn iter(&self) -> Iter<'_, T> {
        self.data.iter()
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns [`true`] if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clears the set, removing all values.
    pub fn clear(&mut self) {
        self.data.clear()
    }
}

impl<T> Set<T>
where
    T: Eq + Hash + Scalar,
{
    /// Reserves capacity for at least `additional` more elements to be inserted
    /// in the `HashSet`. The collection may reserve more space to speculatively
    /// avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional)
    }

    /// Shrinks the capacity of the set as much as possible. It will drop
    /// down as much as possible while maintaining the internal rules
    /// and possibly leaving some space in accordance with the resize policy.
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit()
    }

    /// Returns `true` if the set contains a value.
    ///
    /// The value may be any borrowed form of the set's value type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the value type.
    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.data.contains(value)
    }

    /// Returns a reference to the value in the set, if any, that is equal to the given value.
    ///
    /// The value may be any borrowed form of the set's value type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the value type.
    pub fn get<Q>(&self, value: &Q) -> Option<&T>
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.data.get(value)
    }

    /// Adds a value to the set.
    ///
    /// Returns whether the value was newly inserted. That is:
    ///
    /// - If the set did not previously contain this value, `true` is returned.
    /// - If the set already contained this value, `false` is returned,
    ///   and the set is not modified: original value is not replaced,
    ///   and the value passed as argument is dropped.
    pub fn insert(&mut self, value: T) -> bool {
        self.data.insert(value)
    }

    /// Removes a value from the set. Returns whether the value was
    /// present in the set.
    ///
    /// The value may be any borrowed form of the set's value type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the value type.
    pub fn remove<Q>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.data.remove(value)
    }

    /// Removes and returns the value in the set, if any, that is equal to the given one.
    ///
    /// The value may be any borrowed form of the set's value type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the value type.
    pub fn take<Q>(&mut self, value: &Q) -> Option<T>
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.data.take(value)
    }
}

impl<T> IntoIterator for Set<T> {
    type Item = T;

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<T: Display> Display for Set<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();

        for v in self.iter() {
            s.push_str(v.to_string().as_str());
            s.push_str(", ");
        }

        if !s.is_empty() {
            s.pop();
            s.pop();
        }

        write!(f, "{{{}}}", s)
    }
}

impl<T: Eq + Hash> PartialEq for Set<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T: Eq + Hash> Eq for Set<T> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetImpl {
    Bool(Set<Bool>),
    Char(Set<Char>),
    Short(Set<Short>),
    Int(Set<Int>),
    Long(Set<Long>),

    Date(Set<Date>),
    Month(Set<Month>),
    Time(Set<Time>),
    Minute(Set<Minute>),
    Second(Set<Second>),
    DateTime(Set<DateTime>),
    Timestamp(Set<Timestamp>),
    NanoTime(Set<NanoTime>),
    NanoTimestamp(Set<NanoTimestamp>),

    Float(Set<Float>),
    Double(Set<Double>),

    Symbol(Set<Symbol>),
    String(Set<DolphinString>),

    DateHour(Set<DateHour>),

    Decimal32(Set<Decimal32>),
    Decimal64(Set<Decimal64>),
    Decimal128(Set<Decimal128>),
}

impl SetImpl {
    pub const FORM_BYTE: DataForm = DataForm::Set;

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

impl Constant for SetImpl {
    fn data_form(&self) -> DataForm {
        Self::data_form()
    }

    fn data_type(&self) -> DataType {
        self.data_type()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl Serialize for SetImpl {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let keys: VectorImpl = self.clone().into();

        (keys.data_type(), self.data_form()).serialize(buffer)?;

        keys.serialize(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let keys: VectorImpl = self.clone().into();

        (keys.data_type(), self.data_form()).serialize_le(buffer)?;

        keys.serialize_le(buffer)?;

        Ok(0)
    }
}

impl Deserialize for SetImpl {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let v = deserialize_vector(reader).await?;

        *self = v.try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "unsupported set type from server",
            )
        })?;

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let v = deserialize_vector_le(reader).await?;

        *self = v.try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "unsupported set type from server",
            )
        })?;

        Ok(())
    }
}

macro_rules! from_impl {
    (DolphinString, DolphinString) => {
        from_impl!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<Vector<$struct_name>> for Set<$struct_name> {
            fn from(value: Vector<$struct_name>) -> Self {
                let mut res = Self::with_capacity(value.len());
                for val in value {
                    res.insert(val);
                }
                res
            }
        }

        impl From<Set<$struct_name>> for Vector<$struct_name> {
            fn from(value: Set<$struct_name>) -> Self {
                let mut res = Self::with_capacity(value.len());
                for val in value {
                    res.push(val);
                }
                res
            }
        }

        impl From<Set<$struct_name>> for SetImpl {
            fn from(value: Set<$struct_name>) -> Self {
                Self::$enum_name(value)
            }
        }

        impl From<Set<$struct_name>> for VectorImpl {
            fn from(value: Set<$struct_name>) -> Self {
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

macro_rules! dispatch_from {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl From<SetImpl> for VectorImpl {
            fn from(value: SetImpl) -> Self {
                match value {
                    $(
                        SetImpl::$enum_name(val) => val.into(),
                    )*
                }
            }
        }

        impl TryFrom<VectorImpl> for SetImpl {
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

macro_rules! dispatch_len {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl SetImpl {
            pub fn len(&self) -> usize {
                match self {
                    $(
                        SetImpl::$enum_name(s) => s.len(),
                    )*
                }
            }

            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }
        }
    };
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl SetImpl {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    $(
                        $struct_name::DATA_BYTE => Some(Self::$enum_name(Set::new())),
                    )*
                    _ => None,
                }
            }
        }
    };
}

macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl SetImpl {
            pub fn data_type(&self) -> DataType {
                match self {
                    $(
                        SetImpl::$enum_name(_) => $struct_name::data_type(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for SetImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        SetImpl::$enum_name(v) => write!(f, "{}", v),
                    )*
                }
            }
        }
    };
}

macro_rules! for_all_sets {
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

for_all_sets!(from_impl);

for_all_sets!(dispatch_from);

for_all_sets!(dispatch_len);

for_all_sets!(dispatch_reflect);

for_all_sets!(dispatch_data_type);

for_all_sets!(dispatch_display);
