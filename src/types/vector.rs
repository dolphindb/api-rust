use super::{
    any::Any, array_vector::*, decimal::*, for_all_types, primitive::*, temporal::*, Constant,
    ConstantImpl, DataForm, DataType, DecimalInterface, NotDecimal, ScalarImpl,
};
use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};
use byteorder::{WriteBytesExt, BE, LE};
use bytes::BufMut;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::{
    any::type_name,
    collections::HashMap,
    fmt::{self, Display},
    ops::{Deref, DerefMut, Index, IndexMut},
    slice::{Iter, IterMut, SliceIndex},
    vec::IntoIter,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

#[derive(Default, Debug, Clone)]
pub struct Vector<S> {
    data: Vec<S>,
}

pub type VoidVector = Vector<Void>;
pub type BoolVector = Vector<Bool>;
pub type CharVector = Vector<Char>;
pub type ShortVector = Vector<Short>;
pub type IntVector = Vector<Int>;
pub type LongVector = Vector<Long>;
pub type DateVector = Vector<Date>;
pub type MonthVector = Vector<Month>;
pub type TimeVector = Vector<Time>;
pub type MinuteVector = Vector<Minute>;
pub type SecondVector = Vector<Second>;
pub type DateTimeVector = Vector<DateTime>;
pub type TimestampVector = Vector<Timestamp>;
pub type NanoTimeVector = Vector<NanoTime>;
pub type NanoTimestampVector = Vector<NanoTimestamp>;
pub type FloatVector = Vector<Float>;
pub type DoubleVector = Vector<Double>;
pub type SymbolVector = Vector<Symbol>;
pub type StringVector = Vector<DolphinString>;
pub type AnyVector = Vector<Any>;
pub type DateHourVector = Vector<DateHour>;
pub type BlobVector = Vector<Blob>;
pub type Decimal32Vector = Vector<Decimal32>;
pub type Decimal64Vector = Vector<Decimal64>;
pub type Decimal128Vector = Vector<Decimal128>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorImpl {
    Void(Vector<Void>),
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
    Timestamp(Vector<Timestamp>),
    NanoTime(Vector<NanoTime>),
    NanoTimestamp(Vector<NanoTimestamp>),

    Float(Vector<Float>),
    Double(Vector<Double>),

    Symbol(Vector<Symbol>),
    String(Vector<DolphinString>),

    Any(Vector<Any>),
    DateHour(Vector<DateHour>),

    Blob(Vector<Blob>),

    Decimal32(Vector<Decimal32>),
    Decimal64(Vector<Decimal64>),
    Decimal128(Vector<Decimal128>),

    ArrayVector(ArrayVectorImpl),
}

impl VectorImpl {
    pub const FORM_BYTE: DataForm = DataForm::Vector;

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
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

impl<T> FromIterator<T> for Vector<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let data = Vec::from_iter(iter);
        Self { data }
    }
}

impl<S> IntoIterator for Vector<S> {
    type Item = S;

    type IntoIter = IntoIter<S>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<S: PartialEq> PartialEq for Vector<S> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<S: Eq> Eq for Vector<S> {}

impl<S> From<Vec<S>> for Vector<S> {
    fn from(value: Vec<S>) -> Self {
        Self { data: value }
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

    /// Returns an iterator over the slice.
    ///
    /// The iterator yields all items from start to end.
    pub fn iter(&self) -> Iter<'_, S> {
        self.data.iter()
    }

    /// Returns an iterator that allows modifying each value.
    ///
    /// The iterator yields all items from start to end.
    pub fn iter_mut(&mut self) -> IterMut<'_, S> {
        self.data.iter_mut()
    }
}

impl<S: Clone> Vector<S> {
    ///Resizes the vector in-place so that `len` is equal to `new_len`.
    pub fn resize(&mut self, new_len: usize, value: S) {
        self.data.resize(new_len, value);
    }
}

impl<S: Primitive> Vector<S> {
    // impl<S: Scalar> From<S::RefType> for Vector<S> would conflict with std blanket implementations.
    // Implement it as function instead.
    /// Constructs a new [`Vector`] by cloning raw data arrays.
    pub fn from_raw(raw: &[S::RefType<'_>]) -> Self {
        let mut data = Vec::with_capacity(raw.len());

        for val in raw.iter() {
            data.push(S::new(S::to_owned(*val)));
        }

        Self { data }
    }

    /// Appends a primitive element to the back of a collection.
    pub fn push_raw(&mut self, value: S::RefType<'_>) {
        self.data.push(S::new(S::to_owned(value)))
    }
}

impl Vector<Any> {
    pub fn data_type(&self) -> DataType {
        Any::DATA_BYTE
    }

    /// Appends a primitive element to the back of a collection.
    pub fn push_raw(&mut self, value: ConstantImpl) {
        self.data.push(value.into())
    }
}

impl<S: Display> Display for Vector<S> {
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

        write!(f, "[{}]", s)
    }
}

impl From<Vector<Any>> for VectorImpl {
    fn from(value: Vector<Any>) -> Self {
        Self::Any(value)
    }
}

impl TryFrom<VectorImpl> for Vector<Any> {
    type Error = Error;

    fn try_from(value: VectorImpl) -> Result<Self, Self::Error> {
        match value {
            VectorImpl::Any(v) => Ok(v),
            _ => Err(Error::InvalidConvert {
                from: type_name::<Vec<ScalarImpl>>().to_string(),
                to: type_name::<Self>().to_string(),
            }),
        }
    }
}

impl<S> Serialize for Vector<S>
where
    S: Serialize + NotDecimal,
{
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        for v in self.data.iter() {
            v.serialize(buffer)?;
        }
        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        for v in self.data.iter() {
            v.serialize_le(buffer)?;
        }
        Ok(0)
    }
}

macro_rules! serialize_decimal {
    ($raw_type:tt, $write_func:ident, $func_name:ident, $endian:tt) => {
        fn $func_name<B>(&self, buffer: &mut B) -> Result<usize>
        where
            B: bytes::BufMut,
        {
            let mut writer = buffer.writer();

            let mut replica = self.clone();
            replica.scale_to_same();

            let _ = writer.write_u32::<$endian>(replica.max_scale());

            for mantissa in replica.iter().map(|d| d.mantissa().unwrap_or($raw_type::MIN)) {
                let _ = writer.$write_func::<$endian>(mantissa);
            }

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
    (i32, Decimal32Vector, write_i32),
    (i64, Decimal64Vector, write_i64),
    (i128, Decimal128Vector, write_i128)
);

impl<S> Deserialize for Vector<S>
where
    S: Deserialize + NotDecimal,
{
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        for slot in self.iter_mut() {
            slot.deserialize(reader).await?;
        }
        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        for slot in self.iter_mut() {
            slot.deserialize_le(reader).await?;
        }
        Ok(())
    }
}

macro_rules! deserialize_decimal {
    ($struct_name:ident, $raw_type:tt, $read_scale:ident, $read_func:ident, $func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let scale = reader.$read_scale().await?;

            for slot in self.iter_mut() {
                let mantissa = reader.$read_func().await?;
                *slot = if mantissa != $raw_type::MIN {
                    $struct_name::from_raw(mantissa, scale as u32).ok_or(Error::ConstraintsViolated("decimal scale overflow".into()))?
                } else {
                    $struct_name::default()
                };
            }

            Ok(())
        }
    };

    ($(($raw_type:tt, $struct_name:ident, $read_func:ident, $read_func_le:ident)), *) => {
        $(
            impl Deserialize for Vector<$struct_name> {
                deserialize_decimal!($struct_name, $raw_type, read_i32, $read_func, deserialize);
                deserialize_decimal!($struct_name, $raw_type, read_i32_le, $read_func_le, deserialize_le);
            }
        )*
    };
}

deserialize_decimal!(
    (i32, Decimal32, read_i32, read_i32_le),
    (i64, Decimal64, read_i64, read_i64_le),
    (i128, Decimal128, read_i128, read_i128_le)
);

macro_rules! try_from_impl {
    (DolphinString, DolphinString) => {
        try_from_impl!(DolphinString, String);
    };

    ($struct_name:ident, $enum_name:ident) => {
        impl From<Vector<$struct_name>> for VectorImpl {
            fn from(value: Vector<$struct_name>) -> Self {
                Self::$enum_name(value)
            }
        }

        impl From<Vector<$struct_name>> for Vec<ScalarImpl> {
            fn from(value: Vector<$struct_name>) -> Self {
                value.data.into_iter().map(|v| v.into()).collect::<Vec<_>>()
            }
        }

        impl TryFrom<VectorImpl> for Vector<$struct_name> {
            type Error = Error;

            fn try_from(value: VectorImpl) -> Result<Self> {
                match value {
                    VectorImpl::$enum_name(v) => Ok(v),
                    _ => Err(Error::InvalidConvert {
                        from: value.data_type().to_string(),
                        to: stringify!($struct_name).to_string(),
                    }),
                }
            }
        }
    };

    ($(($raw_type:tt, $enum_name:ident)), *) => {
        $(
            try_from_impl!($enum_name, $enum_name);
        )*
    };
}

macro_rules! to_constant_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<Vector<$struct_name>> for ConstantImpl {
            fn from(value: Vector<$struct_name>) -> Self {
                let s: VectorImpl = value.into();
                s.into()
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            to_constant_impl!($raw_type, $struct_name);
        )*
    };
}

for_all_types!(try_from_impl);

for_all_types!(to_constant_impl);

macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub fn data_type(&self) -> DataType {
                match self {
                    $(
                        VectorImpl::$enum_name(_) => $struct_name::data_type(),
                    )*
                    VectorImpl::ArrayVector(v) => v.data_type(),
                }
            }
        }
    };
}

macro_rules! dispatch_len {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub fn len(&self) -> usize {
                match self {
                    $(
                        VectorImpl::$enum_name(s) => s.len(),
                    )*
                    VectorImpl::ArrayVector(v) => v.len(),
                }
            }
        }
    };
}

macro_rules! dispatch_resize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub fn resize(&mut self, new_len: usize) {
                match self {
                    $(
                        VectorImpl::$enum_name(s) => s.resize(new_len, $struct_name::default()),
                    )*
                    VectorImpl::ArrayVector(v) => v.resize(new_len)
                }
            }
        }
    };
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub(crate) fn serialize_data<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        VectorImpl::$enum_name(s) => s.serialize(buffer),
                    )*
                    VectorImpl::ArrayVector(v) => v.serialize_data(buffer),
                }
            }

            pub(crate) fn serialize_data_le<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        VectorImpl::$enum_name(s) => s.serialize_le(buffer),
                    )*
                    VectorImpl::ArrayVector(v) => v.serialize_data_le(buffer),
                }
            }
        }
    };
}

macro_rules! dispatch_deserialize {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub(crate) async fn deserialize_data<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    $(
                        VectorImpl::$enum_name(s) => s.deserialize(reader).await,
                    )*
                    VectorImpl::ArrayVector(v) => v.deserialize_data(reader).await,
                }
            }

            pub(crate) async fn deserialize_data_le<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    $(
                        VectorImpl::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                    VectorImpl::ArrayVector(v) => v.deserialize_data_le(reader).await,
                }
            }
        }
    };
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl VectorImpl {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    $(
                        $struct_name::DATA_BYTE => Some(Self::$enum_name(Vector::new())),
                    )*
                    _ => panic!("Unsupported data type"),
                }
            }
        }
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for VectorImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        VectorImpl::$enum_name(v) => write!(f, "{}", v),
                    )*
                    VectorImpl::ArrayVector(v) => write!(f, "{}", v),
                }
            }
        }
    };
}

macro_rules! for_all_vectors {
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
            (Any, Any),
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

for_all_vectors!(dispatch_data_type);

for_all_vectors!(dispatch_len);

for_all_vectors!(dispatch_resize);

for_all_vectors!(dispatch_serialize);

for_all_vectors!(dispatch_deserialize);

for_all_vectors!(dispatch_reflect);

for_all_vectors!(dispatch_display);

impl Constant for VectorImpl {
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

impl Serialize for VectorImpl {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        (self.data_type(), self.data_form()).serialize(buffer)?;

        buffer.put_i32(self.len() as i32);
        buffer.put_i32(1);

        self.serialize_data(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        (self.data_type(), self.data_form()).serialize_le(buffer)?;

        buffer.put_i32_le(self.len() as i32);
        buffer.put_i32_le(1);

        self.serialize_data_le(buffer)
    }
}

impl Deserialize for VectorImpl {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32().await?)
            .map_err(|e| Error::InvalidNumeric(e.to_string()))?;

        let _cols = reader.read_i32().await?;

        self.resize(len);

        self.deserialize_data(reader).await
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let len = usize::try_from(reader.read_i32_le().await?)
            .map_err(|e| Error::InvalidNumeric(e.to_string()))?;

        let _cols = reader.read_i32_le().await?;

        self.resize(len);

        self.deserialize_data_le(reader).await
    }
}

macro_rules! deserialize_symbol_with_base {
    ($read_i32:ident, $read_func:ident, $deserialize_base:ident, $deserialize_symbol:ident) => {
        async fn $deserialize_base<R>(&mut self, reader: &mut R) -> Result<HashMap<i32, Symbol>>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut symbol_base_map = HashMap::new();

            let _symbol_base_id = reader.$read_i32().await?;
            let symbol_base_size = usize::try_from(reader.$read_i32().await?)
                .map_err(|e| Error::InvalidNumeric(e.to_string()))?;

            let mut symbol_base_vec = Vector::<Symbol>::new();
            symbol_base_vec.resize(symbol_base_size, Symbol::default());
            symbol_base_vec.$read_func(reader).await?;
            for (id, s) in symbol_base_vec.into_iter().enumerate() {
                symbol_base_map.insert(id as i32, s);
            }

            Ok(symbol_base_map)
        }

        pub(crate) async fn $deserialize_symbol<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let len = usize::try_from(reader.$read_i32().await?)
                .map_err(|e| Error::InvalidNumeric(e.to_string()))?;

            let _cols = reader.$read_i32().await?;

            // handle symbol base
            let symbol_base_map = self.$deserialize_base(reader).await?;

            let mut symbol_ids = Vector::<Int>::new();
            symbol_ids.resize(len, Int::default());
            symbol_ids.$read_func(reader).await?;

            let s = symbol_ids
                .into_iter()
                .map(|id| id.into_inner())
                .collect::<Option<Vec<_>>>()
                .ok_or(Error::BadResponse(
                    "unexpected null id in symbol vector.".into(),
                ))?
                .into_iter()
                .map(|id| symbol_base_map.get(&id).cloned())
                .collect::<Option<Vec<_>>>()
                .ok_or(Error::BadResponse("unexpected id in symbol vector.".into()))?;

            *self = s.into();

            Ok(())
        }
    };

    () => {
        impl Vector<Symbol> {
            deserialize_symbol_with_base!(
                read_i32,
                deserialize,
                deserialize_symbol_base,
                deserialize_with_symbol_base
            );

            deserialize_symbol_with_base!(
                read_i32_le,
                deserialize_le,
                deserialize_symbol_base_le,
                deserialize_with_symbol_base_le
            );
        }
    };
}

deserialize_symbol_with_base!();

impl<S> Vector<S>
where
    S: DecimalInterface,
{
    pub(crate) fn max_scale(&self) -> u32 {
        self.iter().filter_map(|d| d.scale()).max().unwrap_or(0)
    }

    pub(crate) fn scale_to_same(&mut self) {
        let max_scale = self.iter().filter_map(|d| d.scale()).max();

        if let Some(new_scale) = max_scale {
            self.iter_mut().for_each(|s| s.rescale(new_scale));
        }
    }
}

#[derive(Debug, Clone)]
pub enum PrimitiveType {
    None,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    VecI8(Vec<i8>),
    VecI16(Vec<i16>),
    VecI32(Vec<i32>),
    VecI64(Vec<i64>),
    VecF32(Vec<f32>),
    VecF64(Vec<f64>),
    NaiveDateTime(NaiveDateTime),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
}

impl Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = match self {
            PrimitiveType::Bool(_) => "bool(Bool)",
            PrimitiveType::I8(_) => "i8(Char)",
            PrimitiveType::I16(_) => "i16(Short)",
            PrimitiveType::I32(_) => "i32(Int)",
            PrimitiveType::I64(_) => "i64(Long)",
            PrimitiveType::F32(_) => "f32(Float)",
            PrimitiveType::F64(_) => "f64(Double)",
            PrimitiveType::String(_) => "String(String)",
            PrimitiveType::VecI8(_) => "Vec<i8>(Char[])",
            PrimitiveType::VecI16(_) => "Vec<i16>(Short[])",
            PrimitiveType::VecI32(_) => "Vec<i32>(Int[])",
            PrimitiveType::VecI64(_) => "Vec<i64>(Long[])",
            PrimitiveType::VecF32(_) => "Vec<f32>(Float[])",
            PrimitiveType::VecF64(_) => "Vec<f64>(Double[])",
            PrimitiveType::NaiveDateTime(_) => "NaiveDateTime",
            PrimitiveType::NaiveDate(_) => "NaiveDate",
            PrimitiveType::NaiveTime(_) => "NaiveDate",
            PrimitiveType::None => "None",
        };
        write!(f, "{type_name}")
    }
}

// (rust type, ddb type, enum for scalar, enum for array vector)
macro_rules! for_array_types {
    ($macro:tt) => {
        $macro!(
            (i8, Char, I8, VecI8),
            (i16, Short, I16, VecI16),
            (i32, Int, I32, VecI32),
            (i64, Long, I64, VecI64),
            (f32, Float, F32, VecF32),
            (f64, Double, F64, VecF64)
        );
    };
}

// rust type, ddb type, PrimitiveType name
macro_rules! for_primitive_types {
    ($macro:tt) => {
        $macro!(
            (bool, Bool, Bool),
            (String, String, String),
            (NaiveDateTime, DateTime, NaiveDateTime),
            (NaiveDate, Date, NaiveDate),
            (NaiveTime, Time, NaiveTime)
        );
    };
}

impl From<()> for PrimitiveType {
    fn from(_: ()) -> Self {
        Self::None
    }
}

macro_rules! from_for_primitive_type {
    ($type_name:ident, $enum_name:ident) => {
        impl From<$type_name> for PrimitiveType {
            fn from(value: $type_name) -> Self {
                Self::$enum_name(value)
            }
        }
    };

    // ddb_type is ignored
    ($(($rust_type:ident, $ddb_type:ident, $enum_name:tt)), *) => {
        $(
            from_for_primitive_type!($rust_type, $enum_name);
        )*
    };
}

macro_rules! from_for_array_type {
    ($type_name:ident, $enum_name:ident, $array_enum_name:ident) => {
        impl From<$type_name> for PrimitiveType {
            fn from(value: $type_name) -> Self {
                Self::$enum_name(value)
            }
        }
        impl From<Vec<$type_name>> for PrimitiveType {
            fn from(value: Vec<$type_name>) -> Self {
                Self::$array_enum_name(value)
            }
        }
    };

    // ddb_name is ignored
    ($(($type_name:tt, $ddb_name:ident, $enum_name:ident, $array_enum_name:ident)), *) => {
        $(
            from_for_array_type!($type_name, $enum_name, $array_enum_name);
        )*
    };
}

for_primitive_types!(from_for_primitive_type);
for_array_types!(from_for_array_type);

impl VectorImpl {
    pub fn push(&mut self, value: ConstantImpl) -> Result<(), String> {
        if self.data_type() == Any::data_type() {
            let data = match self {
                VectorImpl::Any(data) => data,
                _ => unreachable!(),
            };
            data.push(value.into());
            return Ok(());
        }

        if value.data_form() != ScalarImpl::FORM_BYTE || self.data_type() != value.data_type() {
            return Err("invalid type for vector.".to_string());
        }

        let s = match value {
            ConstantImpl::Scalar(s) => s,
            _ => unreachable!(),
        };

        self.push_scalar(s);
        Ok(())
    }

    pub fn push_primitive_type(&mut self, value: PrimitiveType) -> Result<(), Error> {
        macro_rules! push_simple_type {
            ($ddb_type:ident, $enum_name:ident) => {
                if let VectorImpl::$ddb_type(v) = self {
                    if let PrimitiveType::$enum_name(tmp) = value {
                        v.push(tmp.into());
                        return Ok(());
                    } else if let PrimitiveType::None = value {
                        v.push($ddb_type::default().into());
                        return Ok(());
                    }
                }
            };
            ($(($rust_type:tt, $ddb_type:ident, $enum_name:ident)), *) => {
                $(
                    push_simple_type!($ddb_type, $enum_name);
                )*
            };
        }

        macro_rules! push_array_type {
            ($ddb_type:ident, $enum_name:ident, $array_enum_name:ident) => {
                if let VectorImpl::$ddb_type(v) = self {
                    if let PrimitiveType::$enum_name(tmp) = value {
                        v.push(tmp.into());
                        return Ok(());
                    } else if let PrimitiveType::None = value {
                        v.push($ddb_type::default().into());
                        return Ok(());
                    }
                }
                if let VectorImpl::ArrayVector(ArrayVectorImpl::$ddb_type(a)) = self {
                    if let PrimitiveType::$array_enum_name(tmp) = value {
                        a.push(tmp);
                        return Ok(());
                    }
                }
            };
            // rust_type is ignored
            ($(($rust_type:tt, $ddb_type:tt, $enum_name:ident, $array_enum_name:ident)), *) => {
                $(
                    push_array_type!($ddb_type, $enum_name, $array_enum_name);
                )*
            };
        }

        for_primitive_types!(push_simple_type);
        for_array_types!(push_array_type);

        return Err(Error::InvalidConvert {
            from: value.to_string(),
            to: self.data_type().to_string(),
        });
    }

    pub fn push_unchecked(&mut self, value: ConstantImpl) {
        if self.data_type() == Any::data_type() {
            let data = match self {
                VectorImpl::Any(data) => data,
                _ => unreachable!(),
            };
            data.push(value.into());
            return;
        }

        let s = match value {
            ConstantImpl::Scalar(s) => s,
            _ => unreachable!(),
        };

        self.push_scalar(s);
    }
}

macro_rules! deserialize_vector {
    ($func_name:ident, $deserialize_func:ident) => {
        pub(crate) async fn $func_name<R>(reader: &mut R) -> Result<VectorImpl>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut type_form = (0u8, 0u8);
            type_form.$deserialize_func(reader).await?;

            let (data_type, data_form) = type_form;

            if data_form != VectorImpl::FORM_BYTE as u8 {
                return Err(Error::InvalidData {
                    expect: VectorImpl::FORM_BYTE.to_string(),
                    actual: data_form.to_string(),
                });
            }

            let data_type = data_type.try_into().unwrap();
            let mut vecs = VectorImpl::from_type(data_type).unwrap();

            vecs.$deserialize_func(reader).await?;

            Ok(vecs)
        }
    };

    ($(($func_name:ident, $deserialize_func:ident)), *) => {
        $(
            deserialize_vector!($func_name, $deserialize_func);
        )*
    };
}

deserialize_vector!(
    (deserialize_vector, deserialize),
    (deserialize_vector_le, deserialize_le)
);
