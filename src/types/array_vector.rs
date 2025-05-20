use super::{Constant, ConstantImpl, DataForm, DataType};
use crate::{
    error::{Error, Result},
    types::VectorImpl,
    Deserialize, Serialize,
};
use std::fmt::{self, Display};
use tokio::io::AsyncBufReadExt;

#[derive(Default, Debug, Clone)]
pub struct ArrayVector<S> {
    data: Vec<S>,
    index: Vec<usize>,
}

impl<S: PartialEq> PartialEq for ArrayVector<S> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data && self.index == other.index
    }
}

impl<S: PartialEq> Eq for ArrayVector<S> {}

pub type CharArrayVector = ArrayVector<i8>;
pub type ShortArrayVector = ArrayVector<i16>;
pub type IntArrayVector = ArrayVector<i32>;
pub type LongArrayVector = ArrayVector<i64>;
pub type FloatArrayVector = ArrayVector<f32>;
pub type DoubleArrayVector = ArrayVector<f64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayVectorImpl {
    Char(CharArrayVector),
    Short(ShortArrayVector),
    Int(IntArrayVector),
    Long(LongArrayVector),
    Float(FloatArrayVector),
    Double(DoubleArrayVector),
}

impl ArrayVectorImpl {
    pub const FORM_BYTE: DataForm = DataForm::Vector;

    pub fn data_type(&self) -> DataType {
        match self {
            ArrayVectorImpl::Char(_v) => DataType::CharArray,
            ArrayVectorImpl::Short(_v) => DataType::ShortArray,
            ArrayVectorImpl::Int(_v) => DataType::IntArray,
            ArrayVectorImpl::Long(_v) => DataType::LongArray,
            ArrayVectorImpl::Float(_v) => DataType::FloatArray,
            ArrayVectorImpl::Double(_v) => DataType::DoubleArray,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

macro_rules! vector_interface {
    ($(($data_type:tt)), *) => {
        impl ArrayVectorImpl {
            pub(crate) fn resize(&mut self, new_len: usize)
            {
                match self {
                $(
                    ArrayVectorImpl::$data_type(v) => v.resize(new_len),
                )*
                }
            }
        }
    };
}

vector_interface!((Char), (Short), (Int), (Long), (Float), (Double));

// blanket ArrayVector implementations for all Scalar instances
impl<S> ArrayVector<S> {
    /// Constructs a new, empty [`ArrayVector`].
    pub fn new() -> Self {
        Self {
            data: vec![],
            index: vec![],
        }
    }

    /// Clears the vector, removing all values.
    pub fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
    }

    /// Returns the number of elements in the vector, also referred to as its 'length'.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns [`true`] if the vector contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Appends an element to the back of a collection.
    pub fn push(&mut self, value: Vec<S>) {
        self.data.extend(value);
        self.index.push(self.data.len());
    }
}

impl<S: Clone> ArrayVector<S> {
    pub(crate) fn resize(&mut self, new_len: usize) {
        let mut index = 0;
        if !self.is_empty() {
            index = *self.index.last().unwrap();
        }
        self.index.resize(new_len, index);
    }
}

impl<S: Display> Display for ArrayVector<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        let mut i = 0usize;
        let mut prev_index = 0usize;
        for index in self.index.iter() {
            if *index == prev_index {
                s.push_str("[], ");
                continue;
            }
            s.push_str("[");
            while i < *index {
                s.push_str(self.data[i].to_string().as_str());
                s.push_str(",");
                i += 1;
            }
            if !s.is_empty() {
                s.pop();
            }
            s.push_str("], ");
            prev_index = *index;
        }
        if !s.is_empty() {
            s.pop();
            s.pop();
        }

        write!(f, "[{}]", s)
    }
}

macro_rules! serialize {
    ($(($data_type:tt, $put_le:ident)), *) => {
        $(
            impl Serialize for ArrayVector<$data_type> {
                fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
                where
                    B: bytes::BufMut,
                {
                    _ = buffer;
                    Err(Error::Unsupported { data_form: "ArrayVector".to_owned(), data_type: "ALL".to_owned() })
                }

                fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
                where
                    B: bytes::BufMut,
                {
                    if self.len() == 0 {
                        return Ok(0);
                    }
                    // serialize index
                    buffer.put_u16_le(self.len() as u16); // len
                    buffer.put_u8(4); // sizeof index data
                    buffer.put_i8(0); // no use
                    let mut prev = 0;
                    for index in self.index.iter() {
                        let cnt = *index as u32 - prev;
                        buffer.put_u32_le(cnt);
                        prev = *index as u32;
                    }
                    // serialize data
                    for value in self.data.iter() {
                        buffer.$put_le(*value);
                    }
                    Ok(1)
                }
            }
        )*
    };
}

serialize!(
    (i8, put_i8),
    (i16, put_i16_le),
    (i32, put_i32_le),
    (i64, put_i64_le),
    (f32, put_f32_le),
    (f64, put_f64_le)
);

impl<S> Deserialize for ArrayVector<S> {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        _ = reader;
        panic!("Receiving array vector from server is unsupported now.");
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        _ = reader;
        panic!("Receiving array vector from server is unsupported now.");
    }
}

macro_rules! try_from_impl {
    ($struct_name:ident, $enum_name:ident) => {
        impl From<ArrayVector<$struct_name>> for VectorImpl {
            fn from(value: ArrayVector<$struct_name>) -> Self {
                let array_vector = ArrayVectorImpl::$enum_name(value);
                VectorImpl::ArrayVector(array_vector)
            }
        }
    };

    ($(($raw_type:tt, $enum_name:ident)), *) => {
        $(
            try_from_impl!($raw_type, $enum_name);
        )*
    };
}

macro_rules! to_constant_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<ArrayVector<$raw_type>> for ConstantImpl {
            fn from(value: ArrayVector<$raw_type>) -> Self {
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

macro_rules! for_array_types {
    ($macro:tt) => {
        $macro!(
            (i8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (f32, Float),
            (f64, Double)
        );
    };
}

for_array_types!(try_from_impl);

for_array_types!(to_constant_impl);

macro_rules! dispatch_display {
    ($(($enum_name:ident)),*) => {
        impl Display for ArrayVectorImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(v) => write!(f, "{}", v),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_len {
    ($(($enum_name:ident)),*) => {
        impl ArrayVectorImpl {
            pub fn len(&self) -> usize {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(s) => s.len(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident)),*) => {
        impl ArrayVectorImpl {
            pub(crate) fn serialize_data<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            pub(crate) fn serialize_data_le<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(s) => s.serialize_le(buffer),
                    )*
                }
            }

            pub(crate) async fn deserialize_data<R>(&self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                _ = reader;
                panic!("Receiving array vector from server is unsupported now.");
            }

            pub(crate) async fn deserialize_data_le<R>(&self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                _ = reader;
                panic!("Receiving array vector from server is unsupported now.");
            }
        }
    };
}

macro_rules! for_all_vectors {
    ($macro:tt) => {
        $macro!((Char), (Short), (Int), (Long), (Float), (Double));
    };
}

for_all_vectors!(dispatch_len);

for_all_vectors!(dispatch_serialize);

for_all_vectors!(dispatch_display);

impl Constant for ArrayVectorImpl {
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
