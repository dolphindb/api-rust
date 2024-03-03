use crate::Deserialize;
use crate::Serialize;
use std::io::{Error, ErrorKind};
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::slice::SliceIndex;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::scalar::{for_all_branches, Scalar};
use super::{
    Basic, Bool, Char, Constant, DataForm, DataType, Date, DateHour, DateTime, DolphinString,
    Double, Float, Int, Long, Minute, Month, NanoTime, NanoTimeStamp, NotDecimal, ScalarKind,
    Second, Short, Time, TimeStamp,
};

#[derive(Debug, Clone)]
pub enum VectorKind {
    Void(Vector<()>),
    Bool(Vector<Bool>),
    Char(Vector<Char>),
    Short(Vector<Short>),
    Int(Vector<Int>),
    Long(Vector<Long>),

    Date(Vector<Date>),
    Month(Vector<Month>),
    Time(Vector<Time>),
    Minute(Vector<Minute>),
    Second(Vector<Second>),
    DateTime(Vector<DateTime>),
    TimeStamp(Vector<TimeStamp>),
    NanoTime(Vector<NanoTime>),
    NanoTimeStamp(Vector<NanoTimeStamp>),

    Float(Vector<Float>),
    Double(Vector<Double>),

    String(Vector<DolphinString>),

    DateHour(Vector<DateHour>),
}
// todo any is Vector<ConstantKind> ??

impl VectorKind {
    pub const FORM_BYTE: u8 = 1;

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Default, Debug, Clone)]
pub struct Vector<S> {
    data: Vec<S>,
}

impl<S> Deref for Vector<S> {
    type Target = [S];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data[..]
    }
}

impl<S> DerefMut for Vector<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data[..]
    }
}

impl<S, I> Index<I> for Vector<S>
where
    I: SliceIndex<[S]>,
{
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        Index::index(&**self, index)
    }
}

impl<S, I> IndexMut<I> for Vector<S>
where
    I: SliceIndex<[S]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        IndexMut::index_mut(&mut **self, index)
    }
}

// blanket Vector implementations for all Scalar instances
impl<S> Vector<S> {
    /// Constructs a new, empty [`Vector`].
    pub fn new() -> Self {
        Self { data: vec![] }
    }

    /// Constructs a new, empty [`Vector`] with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Clears the vector, removing all values.
    pub fn clear(&mut self) {
        self.data.clear()
    }

    /// Returns the number of elements in the vector, also referred to as its 'length'.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns [`true`] if the vector contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the first element of the slice, or None if it is empty.
    pub fn first(&self) -> Option<&S> {
        self.data.first()
    }

    /// Returns a mutable pointer to the first element of the slice, or None if it is empty.
    pub fn first_mut(&mut self) -> Option<&mut S> {
        self.data.first_mut()
    }

    /// Returns the last element of the slice, or None if it is empty.
    pub fn last(&self) -> Option<&S> {
        self.data.last()
    }

    /// Returns a mutable pointer to the last item in the slice.
    pub fn last_mut(&mut self) -> Option<&mut S> {
        self.data.last_mut()
    }

    /// Appends an element to the back of a collection.
    pub fn push(&mut self, value: S) {
        self.data.push(value)
    }

    /// Removes the last element from a vector and returns it, or None if it is empty.
    pub fn pop(&mut self) -> Option<S> {
        self.data.pop()
    }

    /// Moves all the elements of other into self, leaving other empty.
    pub fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data)
    }

    /// Removes and returns the element at position index within the vector, shifting all elements after it to the left.
    pub fn remove(&mut self, index: usize) -> S {
        self.data.remove(index)
    }

    /// Removes an element from the vector and returns it.
    pub fn swap_remove(&mut self, index: usize) -> S {
        self.data.swap_remove(index)
    }

    /// Shortens the vector, keeping the first `len` elements and dropping the rest.
    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len)
    }
}

impl<S: Clone> Vector<S> {
    ///Resizes the vector in-place so that `len` is equal to `new_len`.
    pub fn resize(&mut self, new_len: usize, value: S) {
        self.data.resize(new_len, value);
    }
}

impl<S: Scalar> Vector<S> {
    // impl<S: Scalar> From<S::RefType> for Vector<S> would conflict with std blanket implementations.
    // Implement it as function instead.
    /// Constructs a new [`Vector`] by cloning raw data arrays.
    pub fn from_raw(raw: &[S::RefType<'_>]) -> Self {
        let mut data = Vec::with_capacity(raw.len());

        for val in raw.iter() {
            data.push(S::new(S::to_owned(*val))); // todo: optimize?
        }

        Self { data }
    }

    /// Appends a primitive element to the back of a collection.
    pub fn push_raw(&mut self, value: S::RefType<'_>) {
        self.data.push(S::new(S::to_owned(value)))
    }
}

impl<S> Serialize for Vector<S>
where
    S: Scalar + Serialize + NotDecimal,
{
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        for v in self.data.iter() {
            v.serialize(buffer)?;
        }
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        for v in self.data.iter() {
            v.serialize_le(buffer)?;
        }
        Ok(0)
    }
}

impl<S> Deserialize for Vector<S>
where
    S: Scalar + Deserialize,
{
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        for slot in self.iter_mut() {
            slot.deserialize(reader).await?;
        }
        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        for slot in self.iter_mut() {
            slot.deserialize_le(reader).await?;
        }
        Ok(())
    }
}

impl From<Vector<()>> for VectorKind {
    fn from(value: Vector<()>) -> Self {
        Self::Void(value)
    }
}

impl From<Vector<()>> for Vec<ScalarKind> {
    fn from(value: Vector<()>) -> Self {
        value.data.into_iter().map(|v| v.into()).collect::<Vec<_>>()
    }
}

impl TryFrom<Vec<ScalarKind>> for Vector<()> {
    type Error = ();

    fn try_from(value: Vec<ScalarKind>) -> Result<Self, Self::Error> {
        let mut res = Vector::with_capacity(value.len());

        for val in value {
            match val {
                ScalarKind::Void => res.push(()),
                _ => return Err(()),
            }
        }

        Ok(res)
    }
}

macro_rules! dispatch_len {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub fn len(&self) -> usize {
                match self {
                    VectorKind::Void(s) => s.len(),
                    $(
                        VectorKind::$enum_name(s) => s.len(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_resize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub fn resize(&mut self, new_len: usize) {
                match self {
                    VectorKind::Void(s) => s.resize(new_len, ()),
                    $(
                        VectorKind::$enum_name(s) => s.resize(new_len, $struct_name::default()),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub(crate) fn serialize_data<B>(&self, buffer: &mut B) -> Result<usize, ()>
            where
                B: bytes::BufMut,
            {
                match self {
                    VectorKind::Void(s) => s.serialize(buffer),
                    $(
                        VectorKind::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            pub(crate) fn serialize_data_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
            where
                B: bytes::BufMut,
            {
                match self {
                    VectorKind::Void(s) => s.serialize_le(buffer),
                    $(
                        VectorKind::$enum_name(s) => s.serialize_le(buffer),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_deserialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub(crate) async fn deserialize_data<R>(&mut self, reader: &mut R) -> std::io::Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    VectorKind::Void(s) => s.deserialize(reader).await,
                    $(
                        VectorKind::$enum_name(s) => s.deserialize(reader).await,
                    )*
                }
            }

            pub(crate) async fn deserialize_data_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    VectorKind::Void(s) => s.deserialize_le(reader).await,
                    $(
                        VectorKind::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorKind {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    DataType::Void => Some(Self::Void(Vector::new())),
                    $(
                        DataType::$struct_name => Some(Self::$enum_name(Vector::new())),
                    )*
                    _ => None,
                }
            }
        }
    };
}

for_all_branches!(dispatch_len);

for_all_branches!(dispatch_resize);

for_all_branches!(dispatch_serialize);

for_all_branches!(dispatch_deserialize);

for_all_branches!(dispatch_reflect);

impl Constant for VectorKind {
    fn data_category(&self) -> u8 {
        Self::FORM_BYTE
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl Serialize for VectorKind {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        (self.data_type().to_u8(), self.data_category()).serialize(buffer)?;

        buffer.put_i32(self.len() as i32);
        buffer.put_i32(1);

        self.serialize_data(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: bytes::BufMut,
    {
        (self.data_type().to_u8(), self.data_category()).serialize_le(buffer)?;

        buffer.put_i32_le(self.len() as i32);
        buffer.put_i32(1);

        self.serialize_data_le(buffer)
    }
}

impl Deserialize for VectorKind {
    async fn deserialize<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32().await?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let _cols = reader.read_i32().await?;

        self.resize(len);

        self.deserialize_data(reader).await
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> std::io::Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32_le().await?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let _cols = reader.read_i32_le().await?;

        self.resize(len);

        self.deserialize_data_le(reader).await
    }
}

// todo

macro_rules! dispatch_try_from {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl From<VectorKind> for Vec<ScalarKind> {
            fn from(value: VectorKind) -> Self {
                match value {
                    VectorKind::Void(val) => val.into(),
                    $(
                        VectorKind::$enum_name(val) => val.into(),
                    )*
                }
            }
        }

        impl TryFrom<Vec<ScalarKind>> for VectorKind {
            type Error = ();

            fn try_from(value: Vec<ScalarKind>) -> Result<Self, Self::Error> {
                if value.is_empty() {
                    return Ok(Self::Void(Vector::new()));
                }

                match value.first().unwrap() {
                    ScalarKind::Void => Ok(Self::Void(value.try_into()?)),
                    $(
                        ScalarKind::$enum_name(_) => Ok(Self::$enum_name(value.try_into()?)),
                    )*
                }
            }
        }
    };
}
for_all_branches!(dispatch_try_from);

macro_rules! try_from_impl {
    (DolphinString, DolphinString) => {
        try_from_impl!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<Vector<$struct_name>> for VectorKind {
            fn from(value: Vector<$struct_name>) -> Self {
                Self::$enum_name(value)
            }
        }

        impl From<Vector<$struct_name>> for Vec<ScalarKind> {
            fn from(value: Vector<$struct_name>) -> Self {
                value.data.into_iter().map(|v| v.into()).collect::<Vec<_>>()
            }
        }

        impl TryFrom<Vec<ScalarKind>> for Vector<$struct_name> {
            type Error = ();

            fn try_from(value: Vec<ScalarKind>) -> Result<Self, Self::Error> {
                let mut res = Vector::with_capacity(value.len());

                for val in value {
                    match val {
                        ScalarKind::$enum_name(s) => res.push(s),
                        _ => return Err(()),
                    }
                }

                Ok(res)
            }
        }
    };

    ($(($raw_type:tt, $enum_name:ident)), *) => {
        $(
            try_from_impl!($enum_name, $enum_name);
        )*
    };
}
for_all_branches!(try_from_impl);

// implement Basic for VectorKind
macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Basic for VectorKind {
            fn data_type(&self) -> DataType {
                match self {
                    VectorKind::Void(_) => DataType::Void,
                    $(
                        VectorKind::$enum_name(s) => s.data_type(),
                    )*
                }
            }

            fn data_form(&self) -> DataForm {
                DataForm::Vector
            }

            fn size(&self) -> usize {
                self.len()
            }
        }
    };
}
for_all_branches!(dispatch_data_type);

// implement Basic for Vector<S>
impl<S: Scalar> Basic for Vector<S> {
    fn data_type(&self) -> DataType {
        <S as Scalar>::data_type()
    }

    fn data_form(&self) -> DataForm {
        DataForm::Vector
    }

    fn size(&self) -> usize {
        self.len()
    }
}
