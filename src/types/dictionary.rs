use std::borrow::Borrow;
use std::collections::hash_map::{Entry, IntoIter, Iter, IterMut, Keys, Values};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::hash::Hash;
use std::ops::Index;
use tokio::io::AsyncBufReadExt;

use crate::{
    error::{Error, Result},
    Deserialize, Serialize,
};

use super::{
    decimal::*, deserialize_vector, deserialize_vector_le, primitive::*, temporal::*, Any,
    Constant, DataForm, DataType, Scalar, ScalarImpl, Vector, VectorImpl,
};

/// DolphinDB's `Dictionary` implemented base on `std::collections::HashMap` but
/// only bounded on key type, value type is fixed on `Any` to provide flexible api.
/// Prefer `HashMap` for common use instead.
#[derive(Debug, Clone, Default)]
pub struct Dictionary<K> {
    data: HashMap<K, Any>,
}

impl<K> Dictionary<K> {
    /// Creates an empty `Dictionary`.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Creates an empty `Dictionary` with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: HashMap::with_capacity(capacity),
        }
    }

    /// Returns the number of elements the dictionary can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// An iterator visiting all keys in arbitrary order.
    /// The iterator element type is `&'a K`.
    pub fn keys(&self) -> Keys<'_, K, Any> {
        self.data.keys()
    }

    /// An iterator visiting all values in arbitrary order.
    /// The iterator element type is `&'a Any`.
    pub fn values(&self) -> Values<'_, K, Any> {
        self.data.values()
    }

    /// An iterator visiting all key-value pairs in arbitrary order.
    /// The iterator element type is `(&'a K, &'a Any)`.
    pub fn iter(&self) -> Iter<'_, K, Any> {
        self.data.iter()
    }

    /// An iterator visiting all key-value pairs in arbitrary order,
    /// with mutable references to the values.
    /// The iterator element type is `(&'a K, &'a mut Any)`.
    pub fn iter_mut(&mut self) -> IterMut<'_, K, Any> {
        self.data.iter_mut()
    }

    /// Returns the number of elements in the dictionary.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns [`true`] if the dictionary contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clears the dictionary, removing all values.
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl<K> Dictionary<K>
where
    K: Eq + Hash,
{
    /// Reserves capacity for at least `additional` more elements to be inserted
    /// in the `Dictionary`. The collection may reserve more space to speculatively
    /// avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional)
    }

    /// Shrinks the capacity of the dictionary as much as possible. It will drop
    /// down as much as possible while maintaining the internal rules
    /// and possibly leaving some space in accordance with the resize policy.
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit()
    }

    /// Gets the given key's corresponding entry in the dict for in-place manipulation.
    pub fn entry(&mut self, key: K) -> Entry<'_, K, Any> {
        self.data.entry(key)
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<Q>(&self, k: &Q) -> Option<&Any>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.data.get(k)
    }

    /// Returns a mutable reference to the value corresponding to the key.
    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut Any>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.data.get_mut(k)
    }

    /// Returns `true` if the dict contains a value for the specified key.
    pub fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.data.contains_key(k)
    }

    /// Inserts a key-value pair into the dict.
    ///
    /// If the dict did not have this key present, [`None`] is returned.
    ///
    /// If the dict did have this key present, the value is updated, and the old
    /// value is returned. The key is not updated, though; this matters for
    /// types that can be `==` without being identical.
    pub fn insert<V>(&mut self, k: K, v: V) -> Option<Any>
    where
        V: Into<ScalarImpl>,
    {
        let s: ScalarImpl = v.into();
        self.data.insert(k, s.into())
    }

    /// Inserts a key-value pair into the dict.
    ///
    /// If the dict did not have this key present, [`None`] is returned.
    ///
    /// If the dict did have this key present, the value is updated, and the old
    /// value is returned. The key is not updated, though; this matters for
    /// types that can be `==` without being identical.
    pub fn insert_any(&mut self, k: K, v: Any) -> Option<Any> {
        self.data.insert(k, v)
    }

    /// Removes a key from the dict, returning the value at the key if the key was previously in the dict.
    pub fn remove<Q>(&mut self, k: &Q) -> Option<Any>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.data.remove(k)
    }
}

impl<K> Dictionary<K>
where
    K: Scalar,
    VectorImpl: From<Vector<K>>,
{
    fn check_type(&self) -> DataType {
        if self.is_empty() {
            return Any::data_type();
        }

        let mut value_type = self.iter().next().unwrap().1.raw_data_type();
        if self.iter().any(|(_, v)| {
            v.raw_data_type() != value_type || v.0.data_form() != ScalarImpl::data_form()
        }) {
            value_type = Any::data_type();
        }

        value_type
    }

    fn to_vecs(&self) -> (VectorImpl, VectorImpl) {
        let value_type = self.check_type();
        let mut keys = Vector::new();

        if value_type == Any::data_type() {
            let mut values = Vector::new();
            for (k, v) in self.data.iter() {
                keys.push(k.clone());
                values.push(v.clone());
            }
            (keys.into(), values.into())
        } else {
            let mut values = VectorImpl::from_type(value_type).unwrap();
            for (k, v) in self.data.iter() {
                keys.push(k.clone());
                values.push_unchecked(v.0.clone());
            }
            (keys.into(), values)
        }
    }
}

impl<K> Dictionary<K>
where
    K: Scalar + Hash + Eq,
{
    fn from_vecs(keys: Vector<K>, values: VectorImpl) -> Self {
        let mut res = Dictionary::new();

        let any_vector = values.break_up();
        for (k, v) in keys.into_iter().zip(any_vector.into_iter()) {
            res.data.insert(k, v);
        }

        res
    }
}

impl<K, Q> Index<&Q> for Dictionary<K>
where
    K: Hash + Eq + Borrow<Q>,
    Q: Hash + Eq,
{
    type Output = Any;

    /// Returns a reference to the value corresponding to the supplied key.
    ///
    /// # Panics
    ///
    /// Panics if the key is not present in the `Dictionary`.
    fn index(&self, index: &Q) -> &Self::Output {
        self.get(index).expect("no entry found for key")
    }
}

impl<K> IntoIterator for Dictionary<K> {
    type Item = (K, Any);
    type IntoIter = IntoIter<K, Any>;

    /// Creates a consuming iterator, that is, one that moves each key-value
    /// pair out of the dict in arbitrary order. The dict cannot be used after
    /// calling this.
    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<K: Display> Display for Dictionary<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();

        for v in self.iter() {
            s.push_str(format!("{}: {}", v.0, v.1).as_str());
            s.push_str(", ");
        }

        if !s.is_empty() {
            s.pop();
            s.pop();
        }

        write!(f, "{{{}}}", s)
    }
}

impl<K: Eq + Hash> PartialEq for Dictionary<K> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<K: Eq + Hash> Eq for Dictionary<K> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DictionaryImpl {
    Bool(Dictionary<Bool>),
    Char(Dictionary<Char>),
    Short(Dictionary<Short>),
    Int(Dictionary<Int>),
    Long(Dictionary<Long>),

    Date(Dictionary<Date>),
    Month(Dictionary<Month>),
    Time(Dictionary<Time>),
    Minute(Dictionary<Minute>),
    Second(Dictionary<Second>),
    DateTime(Dictionary<DateTime>),
    Timestamp(Dictionary<Timestamp>),
    NanoTime(Dictionary<NanoTime>),
    NanoTimestamp(Dictionary<NanoTimestamp>),

    Float(Dictionary<Float>),
    Double(Dictionary<Double>),

    Symbol(Dictionary<Symbol>),
    String(Dictionary<DolphinString>),

    DateHour(Dictionary<DateHour>),

    Decimal32(Dictionary<Decimal32>),
    Decimal64(Dictionary<Decimal64>),
    Decimal128(Dictionary<Decimal128>),
}

impl DictionaryImpl {
    pub const FORM_BYTE: DataForm = DataForm::Dictionary;

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

impl Constant for DictionaryImpl {
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

impl Serialize for DictionaryImpl {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let (keys, values) = self.to_vecs();

        (values.data_type(), self.data_form()).serialize(buffer)?;

        keys.serialize(buffer)?;
        values.serialize(buffer)?;

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        let (keys, values) = self.to_vecs();

        (values.data_type(), self.data_form()).serialize_le(buffer)?;

        keys.serialize_le(buffer)?;
        values.serialize_le(buffer)?;

        Ok(0)
    }
}

impl Deserialize for DictionaryImpl {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let keys = deserialize_vector(reader, &mut None).await?;
        let values = deserialize_vector(reader, &mut None).await?;

        if keys.len() != values.len() {
            return Err(Error::InvalidData {
                expect: format!("value len: {}", keys.len()),
                actual: values.len().to_string(),
            });
        }

        let data_type = keys.data_type();
        *self = DictionaryImpl::from_vecs(keys, values).ok_or(Error::Unsupported {
            data_form: self.data_form().to_string(),
            data_type: data_type.to_string(),
        })?;

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let keys = deserialize_vector_le(reader, &mut None).await?;
        let values = deserialize_vector_le(reader, &mut None).await?;

        if keys.len() != values.len() {
            return Err(Error::InvalidData {
                expect: format!("value len: {}", keys.len()),
                actual: values.len().to_string(),
            });
        }

        let data_type = keys.data_type();
        *self = DictionaryImpl::from_vecs(keys, values).ok_or(Error::Unsupported {
            data_form: self.data_form().to_string(),
            data_type: data_type.to_string(),
        })?;

        Ok(())
    }
}

macro_rules! dispatch_len {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl DictionaryImpl {
            pub fn len(&self) -> usize {
                match self {
                    $(
                        DictionaryImpl::$enum_name(s) => s.len(),
                    )*
                }
            }

            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }
        }
    };
}

macro_rules! dispatch_data_type {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl DictionaryImpl {
            pub fn data_type(&self) -> DataType {
                match self {
                    $(
                        DictionaryImpl::$enum_name(_) => $struct_name::data_type(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_to_vecs {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl DictionaryImpl {
            fn to_vecs(&self) -> (VectorImpl, VectorImpl) {
                match self {
                    $(
                        DictionaryImpl::$enum_name(d) => d.to_vecs(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_from_vecs {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl DictionaryImpl {
            fn from_vecs(keys: VectorImpl, values: VectorImpl) -> Option<Self> {
                match keys {
                    $(
                        VectorImpl::$enum_name(keys) => Some(DictionaryImpl::$enum_name(
                            Dictionary::<$struct_name>::from_vecs(keys, values))),
                    )*


                    _ => None,
                }
            }
        }
    };
}

macro_rules! dispatch_reflect {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl DictionaryImpl {
            pub(crate) fn from_type(data_type: DataType) -> Option<Self> {
                match data_type {
                    $(
                        $struct_name::DATA_BYTE => Some(Self::$enum_name(Dictionary::<$struct_name>::new())),
                    )*
                    _ => None,
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
        impl From<Dictionary<$struct_name>> for DictionaryImpl {
            fn from(value: Dictionary<$struct_name>) -> Self {
                Self::$enum_name(value)
            }
        }
    };

    ($(($raw_type:tt, $enum_name:ident)), *) => {
        $(
            from_impl!($enum_name, $enum_name);
        )*
    };
}

macro_rules! dispatch_display {
    ($(($enum_name:ident, $struct_name:ident)),*) => {
        impl Display for DictionaryImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        DictionaryImpl::$enum_name(v) => write!(f, "{}", v),
                    )*
                }
            }
        }
    };
}

macro_rules! for_all_dicts {
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

for_all_dicts!(dispatch_len);

for_all_dicts!(dispatch_data_type);

for_all_dicts!(dispatch_to_vecs);

for_all_dicts!(dispatch_from_vecs);

for_all_dicts!(dispatch_reflect);

for_all_dicts!(from_impl);

for_all_dicts!(dispatch_display);
