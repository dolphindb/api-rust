# DolphinDB Rust API 文档

- [DolphinDB Rust API 文档](#dolphindb-rust-api-文档)
  - [1. `ClientBuilder<A>` 类型](#1-clientbuildera-类型)
    - [类型参数](#类型参数)
    - [方法](#方法)
      - [1.1 `new`](#11-new)
      - [1.2 `with_auth`](#12-with_auth)
      - [1.3 `with_ssl`](#13-with_ssl)
      - [1.4 `connect`](#14-connect)
  - [2. `Client` 类型](#2-client-类型)
    - [方法](#方法-1)
      - [2.1 `run_script`](#21-run_script)
      - [2.2 `run_function`](#22-run_function)
      - [2.3 `upload`](#23-upload)
  - [3. 类型系统说明](#3-类型系统说明)
    - [3.1 数据形式](#31-数据形式)
    - [`ConstantKind` 枚举](#constantkind-枚举)
    - [`ScalarKind` 枚举](#scalarkind-枚举)
      - [方法](#方法-2)
        - [`data_type`](#data_type)
        - [`data_form`](#data_form)
      - [实现 `Constant` 特征的方法](#实现-constant-特征的方法)
    - [`VectorKind` 枚举](#vectorkind-枚举)
      - [方法](#方法-3)
        - [`data_type`](#data_type-1)
        - [`resize`](#resize)
      - [实现 `Constant` 特征的方法](#实现-constant-特征的方法-1)
    - [`Pair` 类型](#pair-类型)
      - [方法](#方法-4)
        - [`new`](#new)
        - [`data_type`](#data_type-2)
        - [`first`](#first)
        - [`first_mut`](#first_mut)
        - [`second`](#second)
        - [`second_mut`](#second_mut)
      - [实现 `Constant` 特征的方法](#实现-constant-特征的方法-2)
    - [`Dictionary` 类型](#dictionary-类型)
      - [方法](#方法-5)
      - [实现 `Constant` 特征的方法](#实现-constant-特征的方法-3)
    - [`Set` 类型](#set-类型)
      - [方法](#方法-6)
      - [实现 `Constant` 特征的方法](#实现-constant-特征的方法-4)
    - [3.2 数据类型](#32-数据类型)
      - [`Void`](#void)
  - [4. 特征说明](#4-特征说明)
    - [4.1 `Constant` 特征](#41-constant-特征)
      - [方法](#方法-7)
        - [`data_category`](#data_category)
        - [`len`](#len)
        - [`is_empty`](#is_empty)
    - [4.2 `Scalar` 特征](#42-scalar-特征)
    - [4.3 `IsDecimal` 特征](#43-isdecimal-特征)
    - [4.4 `NotDecimal` 特征](#44-notdecimal-特征)
    - [4.5 `DecimalInterface` 特征](#45-decimalinterface-特征)

## 1. `ClientBuilder<A>` 类型

`ClientBuilder` 用于构建客户端连接，支持链式调用以配置连接的各种参数。

### 类型参数

`A`: 实现 `ToSocketAddrs` 特征，用于指定连接的主机地址。

### 方法

#### 1.1 `new`

```rust
impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub fn new(host: A) -> Self
}
```

作用：构造一个 `ClientBuilder` 实例。

参数：

- `host`: 连接的目标主机地址，必须实现了 `ToSocketAddrs` 特征。

返回值：返回一个新的 `ClientBuilder` 实例。

#### 1.2 `with_auth`

```rust
impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub fn with_auth<U: Into<String>, P: Into<String>>(&mut self, auth: (U, P)) -> &mut Self
}
```

作用：设置认证信息。

参数：

- `auth`: 一个包含用户名和密码的元组，其中用户名和密码都可以被转换成 `String` 类型。
<!-- TODO 直接改成 String 行了，Into 用不着 -->

返回值：返回 `ClientBuilder` 实例的可变引用。

#### 1.3 `with_ssl`

```rust
impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub fn with_ssl(&mut self, ssl: bool) -> &mut Self
}
```

作用：设置是否启用 SSL。

参数：

- `ssl`: 一个布尔值，指示是否启用 SSL。

返回值：返回 `ClientBuilder` 实例的可变引用。

#### 1.4 `connect`

```rust
impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub async fn connect(&self) -> Result<Client>
}
```

作用：建立连接，构造 Client 实例。

返回值：成功时返回 `Client` 实例的 `Result`，失败时返回错误。

## 2. `Client` 类型

`Client` 类型实例代表一个已连接的客户端。

### 方法

#### 2.1 `run_script`

```rust
impl Client {
    pub async fn run_script(&mut self, script: String) -> Result<Vec<ConstantImpl>>
}
```
<!-- 拿走所有权吗？ -->

作用：执行给定的脚本。

参数：

- `script`: 要执行的脚本内容，类型为 `String`。

返回值：成功时返回执行结果的 `Vec<ConstantImpl>`，失败时返回错误。

#### 2.2 `run_function`

```rust
impl Client {
    pub async fn run_function(
        &mut self,
        function: String,
        args: Vec<ConstantImpl>
    ) -> Result<Vec<ConstantImpl>>
}
```
<!-- 拿走所有权吗？ -->

作用：执行指定的函数。

参数：

- `function`: 要执行的函数名称，类型为 `String`。
- `args`: 函数参数列表，类型为 `Vec<ConstantImpl>`。

返回值：成功时返回执行结果的 `Vec<ConstantImpl>`，失败时返回错误。

#### 2.3 `upload`

```rust
impl Client {
    pub async fn upload(
        &mut self,
        variables: HashMap<String, ConstantImpl>
    ) -> Result<Vec<ConstantImpl>>
}
```
<!-- 拿走所有权吗？ -->

作用：上传数据到服务器。

参数：

- `variables`: 要上传的数据，键为变量名，值为 `ConstantImpl` 类型。

返回值：成功时返回结果的 `Vec<ConstantImpl>`，失败时返回错误。<!-- TODO：这结果是什么？  -->

## 3. 类型系统说明

### 3.1 数据形式

### `ConstantKind` 枚举

```rust
pub enum ConstantKind {
    Scalar(ScalarKind),
    Vector(VectorKind),
    Pair(Pair),
    Dictionary(HashMap<ScalarKind, ConstantKind>),
    Set(HashSet<ScalarKind>),
}
```

对 DolphinDB 所有具体类型的抽象。

### `ScalarKind` 枚举

```rust
pub enum ScalarKind {
    Void,
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
    TimeStamp(TimeStamp),
    NanoTime(NanoTime),
    NanoTimeStamp(NanoTimeStamp),
    Float(Float),
    Double(Double),
    String(DolphinString),
    DateHour(DateHour),
    Decimal32(Decimal32),
    Decimal64(Decimal64),
    Decimal128(Decimal128),
}
```

对 DolphinDB 标量类型的抽象。

#### 方法

##### `data_type`

```rust
pub const fn data_type(&self) -> u8
```

返回一个 `u8` 值，表示该数据类型。

##### `data_form`

```rust
pub const fn data_form(&self) -> u8
```

返回一个 `u8` 值，表示该类型的数据形式，标量的数据形式值是 0。

#### 实现 `Constant` 特征的方法

- [`data_category`](#data_category)
- [`len`](#data_category)
- [`is_empty`](#data_category)

### `VectorKind` 枚举

```rust
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
    Decimal32(Vector<Decimal32>),
    Decimal64(Vector<Decimal64>),
    Decimal128(Vector<Decimal128>),
}
```

对 DolphinDB 向量类型的抽象。

#### 方法

##### `data_type`

```rust
pub fn data_type(&self) -> u8
```

返回一个 `u8` 值，表示该数据类型。

<!-- todo data_form ?? -->

##### `resize`

```rust
pub fn resize(&mut self, new_len: usize)
```

设置向量的大小，具体作用和标准库的 `Vec` 相同。

#### 实现 `Constant` 特征的方法

- [`data_category`](#data_category)
- [`len`](#data_category)
- [`is_empty`](#data_category)

### `Pair` 类型

对 DolphinDB Pair 类型的抽象。

#### 方法

##### `new`

```rust
pub fn new(pair: (ScalarKind, ScalarKind)) -> Self
```

构造一个 Pair 对象。

##### `data_type`

```rust
pub fn data_type(&self) -> u8
```

返回一个 `u8` 值，表示该数据类型。

<!-- todo data_form?? -->

##### `first`

```rust
pub fn first(&self) -> &ScalarKind
```

返回 Pair 中第一个元素的引用。

##### `first_mut`

```rust
pub fn first_mut(&mut self) -> &mut ScalarKind
```

返回 Pair 中第一个元素的可变引用。

##### `second`

```rust
pub fn second(&self) -> &ScalarKind
```

返回 Pair 中第二个元素的引用。

##### `second_mut`

```rust
pub fn second_mut(&mut self) -> &mut ScalarKind
```

返回 Pair 中第二个元素的可变引用。

#### 实现 `Constant` 特征的方法

- [`data_category`](#data_category)
- [`len`](#data_category)
- [`is_empty`](#data_category)

### `Dictionary` 类型

对 DolphinDB 字典类型的抽象。

#### 方法

<!-- todo -->

#### 实现 `Constant` 特征的方法

- [`data_category`](#data_category)
- [`len`](#data_category)
- [`is_empty`](#data_category)


### `Set` 类型

```rust
```

对 DolphinDB Set 类型的抽象。

#### 方法

<!-- todo -->

#### 实现 `Constant` 特征的方法

- [`data_category`](#data_category)
- [`len`](#data_category)
- [`is_empty`](#data_category)


### 3.2 数据类型

对应 ScalarKind 枚举下的类型。

#### `Void`

<!-- Bool(Bool)
Char(Char)
Short(Short)
Int(Int)
Long(Long)
Date(Date)
Month(Month)
Time(Time)
Minute(Minute)
Second(Second)
DateTime(DateTime)
TimeStamp(TimeStamp)
NanoTime(NanoTime)
NanoTimeStamp(NanoTimeStamp)
Float(Float)
Double(Double)
String(DolphinString)
DateHour(DateHour)
Decimal32(Decimal32)
Decimal64(Decimal64)
Decimal128(Decimal128) -->

<!-- TODO: 实现 Display 特征，to_string -->

## 4. 特征说明

### 4.1 `Constant` 特征

#### 方法

##### `data_category`

```rust
fn data_category(&self) -> u8
```

返回一个 `u8` 值，表示类型所属的类型种类。

##### `len`

```rust
fn len(&self) -> usize
```

返回一个 `usize` 值，表示对象中的元素数。

##### `is_empty`

```rust
fn is_empty(&self) -> bool
```

返回一个 `bool` 值，表示对象是否为空。

<!-- data_type 和 data_form 应该是 ConstantKind 要用的吧，目前的实现有问题 -->

### 4.2 `Scalar` 特征

### 4.3 `IsDecimal` 特征

### 4.4 `NotDecimal` 特征

### 4.5 `DecimalInterface` 特征
