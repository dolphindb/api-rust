### Rust Native API

This project is dedicated to build DolphinDB type system from scratch instead of unsafe call through [libc](https://github.com/rust-lang/libc).

### Rust API Type System

`trait Scalar` defines interfaces and GAT for `Vector`.

```rust
pub trait Scalar: Send + Sync + Clone + Debug + Default + PartialEq + PartialOrd {
    type RawType: Send + Sync + Clone;

    type RefType<'a>: Send + Copy;

    /// data type identifier for serialization.
    fn data_type() -> usize;

    /// Constructs a new, empty [`Scalar`].
    fn new(raw: Self::RawType) -> Self;

    /// Explicitly copy a reference.
    fn to_owned<'a>(ref_data: Self::RefType<'a>) -> Self::RawType;
}
```

Enumerate all scalar types server defined in `ScalarKind`. All raw types are wrapped with `Option` to avoid having to deal with annoying null value every time. Besides, temporal types are stored as `chrono::NativeDateTime` or `chrono::NativeTime` instead of raw `i32` and `i64`. In the same way, Decimal types are handled by excellent `rust_decimal`.

```rust
pub enum ScalarKind {
    Void,
    Bool(Bool),
    Char(Char),
    Short(Short),
    Int(Int),
    Long(Long),
	...
    String(DolphinString),
}

pub struct Bool {
    raw: Option<bool>,
}
```

`struct Vector`  wraps `std::vec::Vec` to implement some interface to make our lives easier. 

```rust
#[derive(Default)]
pub struct Vector<S> {
    data: Vec<S>,
}

// blanket Vector implementations for all Scalar instances
impl<S> Vector<S> {
    /// Constructs a new, empty [`Vector`].
    fn new() -> Self;

    /// Constructs a new, empty [`Vector`] with at least the specified capacity.
    fn with_capacity(capacity: usize) -> Self;

    /// Clears the vector, removing all values.
    fn clear(&mut self);

    /// Returns the number of elements in the vector, also referred to as its 'length'.
    fn len(&self) -> usize;

    /// Returns [`true`] if the vector contains no elements.
    fn is_empty(&self) -> bool;

    /// Returns the first element of the slice, or None if it is empty.
    fn first(&self) -> Option<&S>;

    /// Returns a mutable pointer to the first element of the slice, or None if it is empty.
    fn first_mut(&mut self) -> Option<&mut S>;

    /// Returns the last element of the slice, or None if it is empty.
    fn last(&self) -> Option<&S>;

    /// Returns a mutable pointer to the last item in the slice.
    fn last_mut(&mut self) -> Option<&mut S>;

    /// Appends an element to the back of a collection.
    fn push(&mut self, value: S);

    /// Removes the last element from a vector and returns it, or None if it is empty.
    fn pop(&mut self) -> Option<S>;

    /// Moves all the elements of other into self, leaving other empty.
    fn append(&mut self, other: &mut Self);

    /// Removes and returns the element at position index within the vector, shifting all elements after it to the left.
    fn remove(&mut self, index: usize) -> S;

    /// Removes an element from the vector and returns it.
    fn swap_remove(&mut self, index: usize) -> S;

    /// Shortens the vector, keeping the first `len` elements and dropping the rest.
    fn truncate(&mut self, len: usize);
}

impl<S: Scalar> Vector<S> {
    // impl<S: Scalar> From<S::RefType> for Vector<S> would conflict with std blanket implementations.
    // Implement it as function instead.
    /// Constructs a new [`Vector`] by cloning raw data arrays.
    fn from_raw(raw: &[S::RefType<'_>]) -> Self;

    /// data type identifier for serialization.
    fn data_type() -> usize;

    /// Appends a primitive element to the back of a collection.
    fn push_raw(&mut self, value: S::RefType<'_>);
}
```

Enumerate some Vector types server defined in `VectorKind`.  `Any Vector` and `Array Vector` are not implemented because I can't figure out how to serialize them.

```rust
pub enum VectorKind {
    Void(Vector<()>),
    Bool(Vector<Bool>),
    Char(Vector<Char>),
    Short(Vector<Short>),
    Int(Vector<Int>),
    Long(Vector<Long>),
	...
    String(Vector<DolphinString>),
}
```

`trait Constant` provides some interfaces clients may use.

```rust
pub trait Constant: Send + Sync + Clone {
    /// data type identifier for serialization.
    fn data_type() -> usize;

    /// data category identifier for serialization.
    fn data_category() -> usize;

    /// Returns the number of elements in [`Constant`].
    fn len() -> usize;
}
```

Enumerate some common Constant types server defined in `ConstantKind`.

```rust
pub enum ConstantKind {
    Scalar(ScalarKind),
    Vector(VectorKind),
    Pair(PairKind),
    Dictionary(DictionaryKind),
    Set(SetKind),
}
```

It's a shame that `PairKind` must allocate at heap or there is recursive definition. It's OK to store different types of data inside `PairKind`, so trivial, common sense and deeply rooted in programmers' hearts, but deviated from DolphinDB's implementation, which only supports store the same type data together.

```rust
pub type PairKind = Box<(ConstantKind, ConstantKind)>;
```

`DictionaryKind` and `SetKind` directly exploit std functionalities of `HashMap` and `HashSet`. Again, DolphinDB's implementation only support all keys are the same but Rust API type system allows arbitrary key types.

```rust
pub type DictionaryKind = HashMap<ConstantKind, ConstantKind>;

pub type SetKind =  HashSet<ConstantKind>;
```

#### To Do

1. implement `Table`.
2. implement `Dictionary` and `Set`, don't use type alias.
3. unittest.

#### Known Issues

1. incorrect Deserialize implementation for Decimal type.