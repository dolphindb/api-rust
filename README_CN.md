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

作用：上传数据到服务器。

参数：

- `variables`: 要上传的数据，键为变量名，值为 `ConstantImpl` 类型。

返回值：成功时返回结果的 `Vec<ConstantImpl>`，失败时返回错误。<!-- TODO：这结果是什么？  -->
