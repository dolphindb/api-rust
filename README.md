# DolphinDB Rust API

本教程主要介绍以下内容：

- 项目编译
- 建立DolphinDB连接
- 运行DolphinDB脚本
- 运行DolphinDB函数
- 数据对象介绍
- 上传本地对象到DolphinDB服务器
- 读写DolphinDB数据表

DolphinDB Rust API 目前仅支持Linux开发环境。

### 1.项目编译

#### 1.1 添加环境变量并构建项目

下载整个项目，将API文件夹拷贝至你的Rust项目目录下，使用如下指令添加环境变量：

```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/api/src/apic
```
在你自己项目的Cargo.toml文件里dependencies下加入依赖：
```
[dependencies]
libc = "0.2.0"
ddb = {path = "./api-rust"}
```
然后，运行cargo build重新构建项目。
```bash
cargo build
```


#### 1.2 导入API包

新建或修改main.rs文件并导入DolphinDB Rust API包，可参考tests目录下ddb_test.rs文件，包名已经简写为ddb。数据类型依赖libc。

```Rust

extern crate ddb;
extern crate libc;
use libc::{c_void,c_int,c_char,c_short,c_longlong,c_float,c_double};
use std::ffi::CString;
use std::ffi::CStr;
use std::str;
use crate::ddb::Defobj;

fn main() {
  let conn= ddb::DBConnection::new();
  conn.connect("localhost", 8848, "admin", "123456");
  println!("Hello, world!");
}

```

### 2. 建立DolphinDB连接

DolphinDB Rust API 提供的最核心的对象是DBConnection。Rust应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, user_name, password)|将会话连接到DolphinDB服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run_func(function_name,args)|调用DolphinDB服务器上的函数|
|upload(letiableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|close()|关闭当前会话|

如下脚本声明了一个DBConnection对象。

```Rust
let conn= ddb::DBConnection::new();
```

Rust API通过TCP/IP协议连接到DolphinDB。使用`connect`方法创建连接时，需要提供DolphinDB Server的IP、端口号、用户名及密码，函数返回一个布尔值表示是否连接成功。

```Rust
conn.connect("localhost",8848,"admin","123456");
```

### 3. 运行DolphinDB脚本

通过`run`方法运行DolphinDB脚本：

```Rust
  let v = conn.run("`IBM`RustOG`YHOO");
  println!("{}",v.get_string());
```
输出结果为：
>["IBM","RustOG","YHOO"]

当需要调用DolphinDB内置或用户自定义函数时，若函数所需参数都在服务端，我们可以通过`run`方法直接调用该函数。

例如，对两个向量调用[add](http://www.dolphindb.cn/cn/help/add.html)函数时，若函数所需的两个参数x和y都在服务端被定义，则直接调用`run`：

```Rust
  let sum = conn.run("x = [1,3,5]; y = [2,4,6]; add(x,y)");
  println!("{}",sum.get_string());

```
输出结果为：
>[3,7,11]

### 4. 运行函数

当需要在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数，而函数所需的一个或多个参数需要由Rust客户端提供时，我们可以通过`run_func`方法来调用这类函数。`run_func`的第一个参数为DolphinDB中的函数名，第二个参数是该函数所需的一个或者多个参数，为Constant类型的向量。下面仍以[add](http://www.dolphindb.cn/cn/help/add.html)函数为例，区分两种情况：

- 仅部分参数需由Rust客户端赋值

若变量x已经通过Rust程序在服务器端生成，

```
conn.run("x = [1,3,5]");
```

而参数y要在Rust客户端生成，这时就需要使用“部分应用”方式，把参数x固化在[add](http://www.dolphindb.cn/cn/help/add.html)函数内。具体请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

```Rust
let a2 :[c_int;3] = [9,8,7];
let y0 = ddb::create_vector(ddb::DT_INT, 3);
y0.set_int_slice(0,3,&a2[..]);
let y = y0.to_constant();
let args = ddb::create_vector(ddb::DT_ANY,0);
args.append(y);
let result1 = conn.run_func("add{x,}", args);
println!("{}",result1.get_string());
```
输出结果为：
> [10, 11, 12]

* 所有参数都待由客户端赋值

当所有参数都待由客户端赋值时，直接通过`run_func`方法调用DolphinDB的内置函数：

```Rust
let a1 :[c_int;3] = [1,2,3];
let a2 :[c_int;3] = [9,8,7];
let x0 = ddb::create_vector(ddb::DT_INT, 3);
let y0 = ddb::create_vector(ddb::DT_INT, 3);
x0.set_int_slice(0,3,&a1[..]);
y0.set_int_slice(0,3,&a2[..]);

let args = ddb::create_vector(ddb::DT_ANY,0);
args.append(x0.to_constant());
args.append(y0.to_constant());
let result1 = conn.run_func("add", args);
println!("{}",result1.get_string());

```
输出结果为：
>[10,10,10]

上述例子中，我们使用Rust API中的`create_vector`函数分别创建两个向量，再调用`set_int_slice`函数将Rust语言中int类型的切片赋值给ddb::DT_INT类型的向量。最后调用`to_constant`函数将Vector转换成Constant对象，作为参数上传到DolphinDB server端。

### 5. 数据对象介绍

DolphinDB Rust API 通过Constant这一基本类型接受各种类型的数据，包括DT_INT、DT_FLOAT等。同时，Rust API还提供Vector类和Table类来存放向量和表对象。

#### 5.1 基类

所有其他类型如Table,Vector,Constant等均可使用基类接口Defobj的方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|get_form()|获取对象的数据形式|
|get_type()|获取对象的数据类型|
|size()|获取对象长度|
|get_string()|将对象转换为Rust的字符串|
|is_\<dataform>|校验对象的数据形式|
|is_null()|判断对象是否为Null|
|to_constant()|转换为Constant类型|

#### 5.2 Constant类

Constant类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|set_\<datatype>|将对象设置为DolphinDB中的数据类型|
|get_\<datatype>|将Constant对象转换为Rust中的基本数据类型，注意这里的数据类型为小写开头|
|to_vector()|转换为Vector类型|
|to_table()|转换为Table类型|
|to_set()|转换为Set类型|
|to_dictionary()|转换为Dictionary类型|


具体示例如下：

* get_form、get_type

对Constant对象调用`get_form`方法获取对象的数据形式，调用`get_type`方法获取对象的数据类型。需要注意的是，这两个方法返回的不是字符串，而是数据形式或者数据类型对应的序号，具体对应关系见附录。

```Rust
let x = conn.run("1+1");
x.get_form();
x.get_type();
```
输出结果为如下，其中，form=0代表是form为scalar，type=4代表type为int。

>0 4

* size

size方法，对Vector会获取长度，对Table会获取行数。

```Rust
p.size();
```

* get_\<datatype>

通过`get_<datatype>`系列方法，将Constant对象转换为Rust语言中的常用类型

```Rust
let x = conn.run("1+1");
x.get_bool(); //转换为布尔型
x.get_short();  //int16
x.get_int();  //转换为整形int
x.get_long();  //int64
x.get_float(); //float32
x.get_double();  //float64
x.get_string()  //转换为字符串
```

*  is_\<dataform>

使用`is_<dataform>`系列方法，校验对象的数据形式

```Rust
let x = conn.run("2 3 5");
x.is_scalar();
x.is_vector();
x.is_table();
```

对Constant对象调用`to_vector`可以获得一个Vector对象，Vector类的介绍见5.2小节。

```Rust
let p = conn.run("5 4 8");
let p1 = p.to_vector();
```

类似地，对Constant对象调用`to_table`可以获得一个Table对象, Table类的介绍见5.3小节。

```Rust
let script = "t=table(1..5 as id, rand(5.0, 5) as values);select * from t;"

let p = conn.run(script);
let p1 = p.to_table();
```

#### 5.3 Vector类

Vector(向量)是DolphinDB中常用的类型，也可作为表中的一列，Vector类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|get_name()|获取向量名称|
|set_name()|设置向量名称|
|get(index)|访问向量的元素，返回Constant对象|
|create_vector(dtype, size)|初始化一个指定长度的Vector，返回Vector对象|
|append(Constant)|向Vector尾部追加一个对象|
|remove(n)|移除末尾的n个元素|
|set_\<datatype>_slice|将对应数据类型的切片赋值给向量|
|set_\<datatype>_by_index|将对应数据类型的值赋值给向量index位置|
|append_\<datatype>|将对应数据类型的切片追加到向量尾部|
|get_column_label()|获取列标签，即别名，未设置情况下与向量相同|
|clear()|清空向量|
|get_capacity()|获取向量容量|
|reserve(capacity)|重设向量容量|
|get_unit_length()|获取向量单元长度|
|get_sub_vector(start, len)|截取一段向量|
|fill(start, len, Constant)|从向量start位置开始设置len个Constant值|
|reverse()|反转向量|
|reverse_segment(start, len)|反转向量中从start开始长度为len的片段|
|replace(oldval, newval)|将向量中值为oldval的替换为newval|


具体示例如下：

* get_name, set_name

通过`get_name`获取向量名称，通过`set_name`设置向量名称。

```Rust
p1.set_name("v1");
if  p1.get_name()!= "v1"  { t.Error("set_name Error"); }
```

* get

使用`get`方法获取向量某个下标的元素，从0开始，获取的也是Constant对象

```Rust
p2 = p1.get(0)；
if p2.get_int()!= 1 { t.Error("append Error"); }
```

* create_vector

使用`create_vector`函数创建一个空的Vector，这会返回一个Vector对象,参数type为DolphinDB的数据类型，size为向量的初始长度。

```Rust
p1 = ddb::create_vector(ddb::DT_INT,5);
```
* append

对Vector调用`append`方法可以向Vector尾部push一个对象，这有点类似于C++ vector的push_back方法

```Rust
p1.append(ddb::CreateInt(1));
```

* remove

对Vector调用`remove`，移除末尾的n个元素

```Rust
p1.remove(2);
```

* set_\<datatype>_silce

对Vector调用`set_<datatype>_silce`，将对应数据类型的切片赋值给对应类型的向量，例如：

```Rust
let rowNum = 10;
let v1 = ddb::create_vector(ddb::DT_BOOL, rowNum);
let v2 = ddb::create_vector(ddb::DT_INT, rowNum);
let v3 = ddb::create_vector(ddb::DT_FLOAT, rowNum);
let v4 = ddb::create_vector(ddb::DT_STRING, rowNum);
let mut arr1 :[bool;10] = [true;10];
let mut arr2 :[c_int;10] = [1;10];
let mut arr3 :[c_float;10] = [1.0;10];
let mut arr4 :[&str;10] = ["1";10];

v1.set_bool_slice(0,rowNum,&arr1[..]);
v2.set_int_slice(0,rowNum,&arr2[..]);  
v3.set_float_slice(0,rowNum,&arr3[..]);  
v4.set_string_slice(0,rowNum,&arr4[..]);
```

查看v2.get_string()的结果，结果是一个Int类型的vector。
>[1,1,1,1,1,1,1,1,1,1]


* append_\<datatype>

对Vector调用`append_<datatype>`，将对应数据类型的切片指定长度添加对应类型的向量尾部，例如：
```Rust
const rowNum:usize = 10;
let v1 = ddb::create_vector(ddb::DT_BOOL, 0);
let v2 = ddb::create_vector(ddb::DT_INT, 0);
let v3 = ddb::create_vector(ddb::DT_FLOAT, 0);
let v4 = ddb::create_vector(ddb::DT_STRING, 0);
let mut arr1 :[bool;rowNum] = [true;rowNum];
let mut arr2 :[c_int;rowNum] = [1;rowNum];
let mut arr3 :[c_float;rowNum] = [1.0;rowNum];
let mut arr4 :[&str;rowNum] = ["1";rowNum];
v1.append_Bool(&arr1[..], 10);
v2.append_Int(&arr2[..], 10);  
v3.append_Float(&arr3[..], 10);  
v4.append_String(&arr4[..], 10);
```
查看v4.get_string()的结果，结果是一个String类型的vector。
>["123","123","123","123","123","123","123","123","123","123"]

* set_\<datatype>_by_index

对Vector调用`set_<datatype>_by_index`，将改变对应下标的值，例如：
```Rust
const rowNum:usize = 10;
let v2 = ddb::create_vector(ddb::DT_INT, 10);
let mut arr2 :[c_int;rowNum] = [1;rowNum];
v2.set_int_slice(0, 10, &arr2[..]);  
v2.set_int_by_index(1, 2);
println!("{}",v2.get_string());

```
查看v2.get_string()的结果， 可以看到对应下标的值已经改变。
>[1,2,1,1,1,1,1,1,1,1]

* get_sub_vector

对Vector调用`get_sub_vector`，截取一段向量，例如：
```Rust
const rowNum:usize = 10;
let v = ddb::create_vector(ddb::DT_INT, 10);
let mut arr :[c_int;rowNum] = [1;rowNum];
v.set_int_slice(0, 10, &arr[..]);
let v1 = v.get_sub_vector(0,3);
println!("{}",v1.get_string());
```
查看v1.get_string()的结果如下：
>[1,1,1]

* fill

对Vector调用`fill`，改变一段向量的值，例如：
```Rust
const rowNum:usize = 10;
let v = ddb::create_vector(ddb::DT_STRING, 10);
v.fill(2,3,ddb::create_String("one"));
println!("{}",v.get_string());
```
查看v.get_string()的结果如下：

>[,,"one","one","one",,,,,]


#### 5.4 Table类

Table类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|get_name()|获取表名称|
|set_name()|设置表名称|
|columns()|获取列数|
|rows()|获取行数|
|get_column(index)|访问表的下标为index的列，返回Vector对象|
|get_column_name(index)|获取下标为index列的列名|
|get_column_by_name(name)|通过列名获取列，返回Vector对象|
|create_table(col_name, coltype, size, capacity)|用列名和列创建一个Table，并指定初始行数和预分配内存可容纳行数，返回Table对象|
|set_column_name(index)|设置下标为index列的列名|
|get_column_index(name)|获取列名为name的列下标|
|contain(name)|判断列名为name的列是否存在|
|get_value()|获取表的深拷贝|
|get_string_by_index(index)|将下标为index的列转为string|
|get_window(colstart, collen, rowstart, rowlen)|在表中按下表截取一个window|
|get_member(key)|用Constant类型的key获取对象|
|values()|获取表中的全部值， 返回一个Constant对象|
|keys()|获取表中的全部列名， 返回一个Constant对象|


* get_column

使用`get_column`方法获取表某个下标的列，下标从0开始，返回一个Vector对象

```Rust
t1.get_column(0);
```

* get_column_name

使用`get_column_name`方法获取表某个下标的列名，下标从0开始，返回一个字符串

```Rust
t1.get_column_name(0);
```

* get_columnType

使用`get_columnType`方法获取表中指定列的类型，返回一个DolphinDB数据类型，各数据类型请参考附录。

```Rust
t1.get_columnType(0);
```

* get_column_by_name

使用`get_column_by_name`方法通过列名获取表中的某列，返回一个Vector对象

```Rust
t1.get_column_by_name("v1");
```

* create_table

下面的例子中，使用`create_table`用列名和列创建一个Table，并指定初始行数和预分配内存可容纳行数，返回Table对象，再对Table对象的每一列进行赋值。

```Rust
let col_names : [&str;2] =["id","value"];
let coltypes : [c_int;2] = [ddb::DT_INT, ddb::DT_DOUBLE];
let rowNum = 3;
const colNum:c_int = 2;
let indexCapacity = 3;
let ta = ddb::create_table(&col_names[..], &coltypes[..], rowNum, indexCapacity);
let mut colv:Vec<ddb::Vector> = Vec::new();

for i in 0..(colNum){
  colv.push(ta.get_column(i));
}
let a1:[c_int;3] = [1,2,3];
let a2:[c_double;3] = [1.5,2.7,3.9];
colv[0].set_int_slice(0,3,&a1[..]);
colv[1].set_double_slice(0,3,&a2[..]);
println!("{}",ta.get_string());

```

#### 5.5 create_\<datatype>系列方法

Rust api 提供了一系列的create方法，用于创建DolphinDB的Constant对象：

| 方法名        | 详情          |
|:------------- |:-------------|
|create_Int( c_int )|创建DolphinDB的Int|
|create_Bool( bool )|创建DolphinDB的Bool|
|create_Short( c_short)|创建DolphinDB的Int|
|create_Long( c_longlong )|创建DolphinDB的Int|
|create_Float( c_float )|创建DolphinDB的Int|
|create_Double( c_double )|创建DolphinDB的Int|
|create_String( String||str )|创建DolphinDB的Int|

同时也有一些DolphinDB中的时间类型可以创建

| 方法名        | 详情          |
|:------------- |:-------------|
|create_Date(year, month, day) |创建DolphinDB的Date|
|create_Month(year, month) |创建DolphinDB的Month|
|create_NanoTime(hour, minute, second, nanosecond) |创建DolphinDB的NanoTime|
|create_Time(hour, minute, second, millisecond) |创建DolphinDB的Time|
|create_Second(hour, minute, second) |创建DolphinDB的Second|
|create_Minute(hour, minute) |创建DolphinDB的Minute|
|create_NanoTimestamp(year, month, day, hour, minute, second, nanosecond) |创建DolphinDB的NanoTimestamp|
|create_Timestamp(year, month, millisecond) |创建DolphinDB的Timestamp|
|create_DateTime(year, month, day, hour, minute, second) |创建DolphinDB的DateTime|

#### 5.6 Set类

Set类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|clear()|清空集合|
|remove(Constant)|移除集合中的某个值|
|append(Constant)|往集合中添加某个值|
|interaction(Constant)|求交集|
|inverse(Constant)|调用者变为两个集合的补集|
|get_sub_vector(start, len)|截取一段向量|


#### 5.7 Dictionary类

Dictionary类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|clear()|清空字典|
|count()|字典的key-value对数|
|get_member(key:Constant)|通过key获取value|
|get_keyType()|获取key的类型|
|keys()|获取全部的key|
|values()|获取全部的value|
|remove(key:Constant)|移除字典中的某个键值对|
|set(key:Constant, value:Constant)|设置一个键值对|



### 6. 上传数据对象

调用`upload`方法，可以将一个Constant对象上传到DolphinDB数据库，对于非Constant类型，可以调用`to_constant`方法将其转换为Constant类型对象。

```Rust
let p = conn.run("5 4 8");
let p1 = p.to_vector();
let p2 = p1.to_constant();
conn.upload("vector1",p2);
```

### 7. 读写DolphinDB数据表

使用Rust API的一个重要场景是，用户从其他数据库系统或是第三方Web API中取得数据后存入DolphinDB数据库中。本节将介绍通过Rust API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

* 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不再存在。
* 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
* 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

下面子分别介绍向三种形式的表中追加数据的实例。

首先，我们定义一个`create_demo_table`函数，该函数在Rust环境中创建一个表，具备3个列，类型分别是DT_STRING, DT_DATE和DT_DOUBLE，列名分别为_name, date和price，并向该表中插入10条数据。

```Rust

fn create_demo_table() ->ddb::Table {
  let col_names:[&str;3] =  ["name","date","price"];
  let coltypes:[c_int;3]= [ddb::DT_STRING,ddb::DT_DATE,ddb::DT_DOUBLE];
  const  rowNum:c_int = 10;
  const colNum:c_int = 3;
  let indexCapacity = 11;
  let ta = ddb::create_table(&col_names[..], &coltypes[..], rowNum, indexCapacity);
  let mut colv:Vec<ddb::Vector> = Vec::new();
  for i in 0..(colNum){
     colv.push(ta.get_column(i));
  }
  let mut arr2 :[c_int;10] = [1;rowNum as usize];
  let mut arr3 :[c_double;10] = [1.0;rowNum as usize];
  let mut arr1 :[&str;10] = ["asd";rowNum as usize];


  colv[0].set_string_slice(0,rowNum,&arr1[..]);
  colv[1].set_int_slice(0,rowNum,&arr2[..]);
  colv[2].set_double_slice(0,rowNum,&arr3[..]);
  println!("{}",ta.get_string());
  return ta;
}

```

### 7.1 保存数据到DolphinDB内存表

在DolphinDB中，我们通过[`table`](http://www.dolphindb.cn/cn/help/table.html)函数来创建一个相同结构的内存表，指定表的初始行数和预分配内存可容纳行数、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问table，我们使用[`share`](http://www.dolphindb.cn/cn/help/share1.html)在会话间共享内存表。

```DolphinDB
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);
share t as tglobal;
```

在Rust应用程序中，创建一个表，并调用`to_constant()`方法将表对象转换为Constant类型对象，再通过`run_func`函数调用DolphinDB内置的[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数将demotb表内数据插入到表tglobal中。

```Rust
let ta = create_demo_table();
let args =ddb::create_vector(ddb::DT_ANY,0);
args.append(ta.to_constant());
conn.run_func("tableInsert{tglobal}", args);
let result = conn.run("select * from tglobal");
println!("{}",result.get_string());
```

### 7.2 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

在DolphinDB中使用以下脚本创建一个本地磁盘表，使用[`database`](http://www.dolphindb.cn/cn/help/database1.html)函数创建数据库，调用[`saveTable`](http://www.dolphindb.cn/cn/help/saveTable.html)命令将内存表保存到磁盘中：

```DolphinDB
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]); 
db=database("./demoDB"); 
saveTable(db, t, `dt); 
share t as tDiskGlobal;
```

与7.1小节的方法类似，我们通过将表upload到服务器之后再向磁盘表追加数据。需要注意的是，[`tableInsert`](http://www.dolphindb.cn/cn/help/tableInsert.html)函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行[`saveTable`](http://www.dolphindb.cn/cn/help/saveTable.html)命令。

```Rust
let ta = create_demo_table();
let args =ddb::create_vector(ddb::DT_ANY,0);
args.append(ta.to_constant());
conn.run_func("tableInsert{tDiskGlobal}", args);
conn.run("saveTable(database('./demoDB'),tDiskGlobal,`dt)");
let result = conn.run("select * from tDiskGlobal");
println!("{}",result.get_string());
```

### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过Rust API把数据保存至分布式表。

在DolphinDB中使用以下脚本创建分布式表，脚本中[`database`](http://www.dolphindb.cn/cn/help/database1.html)函数用于创建数据库，对于分布式数据库，路径必须以 dfs 开头。[`createPartitionedTable`](http://www.dolphindb.cn/cn/help/createPartitionedTable.html)函数用于创建分区表。

```DolphinDB
dbPath = "dfs://demoDB";
tablename = `demoTable
db = database(dbPath, VALUE, 1970.01.01..1970.01.03)
pt=db.createPartitionedTable(table(100:0, `name`date`price, [STRING,DATE,DOUBLE]), tablename, `date)
```

DolphinDB提供[`loadTable`](http://www.dolphindb.cn/cn/help/loadTable.html)函数来加载分布式表，通过[`tableInsert`](http://www.dolphindb.cn/cn/help/tableInsert.html)函数追加数据，具体的脚本示例如下：

```Rust
let ta = create_demo_table();
let args =ddb::create_vector(ddb::DT_ANY,0);
args.append(ta.to_constant());
conn.run_func("tableInsert{loadTable('dfs://demoDB', `demoTable)}", args);
```

通过`run`函数查看表内数据：

```Rust
let result = conn.run("select * from loadTable('dfs://demoDB', `demoTable)");
println!("{}",result.get_string());
```
结果为：

```
_name date       price
---- ---------- -----
IBM  1970.01.02 1    
APP  1970.01.02 2    
MS   1970.01.02 3    
FCE  1970.01.02 4    
GOO  1970.01.02 5    
RAG  1970.01.02 6    
FAG  1970.01.02 7    
```
关于追加数据到DolphinDB分区表的实例可以参考examples目录下的[分布式表的数据写入例子](./examples/RWDFSTable.rs)和[分布式表的多线程并行写入例子](./examples/DFSWritingWithMultiThread.rs) 。

### 7.4 读取和使用数据表

在Rust API中，数据表保存为Table对象。由于Table是列式存储，所以若要在Rust API中读取行数据需要先取出需要的列，再取出行。

假设在DolphinDB中如下定义的表，并插入了一些数据在表中：
```DolphinDB
kt = keyedTable(`col_int, 2000:0, `col_int`col_short`col_long`col_float`col_double`col_bool`col_string,  [INT, SHORT, LONG, FLOAT, DOUBLE, BOOL, STRING]);
```
如下例子通过`run`函数查询表内数据，对返回值用to_table方法将其转换为一个Table对象，然后用`get_column_by_name`或`get_column`得到列，再一行行打印数据。
```Rust
let res = conn.run("select top 3 * from kt ");
let res_table = res.to_table();
let col0=res_table.get_column_by_name("col_int");
let col1=res_table.get_column_by_name("col_short");
let col2=res_table.get_column_by_name("col_long");
let col3=res_table.get_column_by_name("col_float");
let col4=res_table.get_column(4);
let col5=res_table.get_column(5);
let col6=res_table.get_column(6);
for i in 0..(res_table.rows()) {
  println!("{},{},{},{},{},{},{}", col0.get(i).get_int(),col1.get(i).get_short(),col2.get(i).get_long(),col3.get(i).get_float(),col4.get(i).get_double(),col5.get(i).get_bool(),col6.get(i).get_string());
}
```
输出结果为：
```Conosle
0,255,10000,133.3,255,true,str
1,255,10001,133.3,255,true,str
2,255,10002,133.3,255,true,str
```

附录
---
数据形式列表（`get_form`函数返回值对应的数据形式）

| 序号       | 数据形式          |
|:------------- |:-------------|
|0|DF_VECTOR
|1|DF_PAIR
|2|DF_MATRIX
|3|DF_SET
|4|DF_DICTIONARY
|5|DF_TABLE
|6|DF_CHART
|7|DF_CHUNK

数据类型列表（`get_type`函数返回值对应的数据类型）

| 序号       | 数据类型          |
|:------------- |:-------------|
|0|DT_VOID
|1|DT_BOOL
|2|DT_CHAR
|3|DT_SHORT
|4|DT_INT
|5|DT_LONG
|6|DT_DATE
|7|DT_MONTH
|8|DT_TIME
|9|DT_MINUTE
|10|DT_SECOND
|11|DT_DATETIME
|12|DT_TIMESTAMP
|13|DT_NANOTIME
|14|DT_NANOTIMESTAMP
|15|DT_FLOAT
|16|DT_DOUBLE
|17|DT_SYMBOL
|18|DT_STRING
|19|DT_UUID
|20|DT_FUNCTIONDEF
|21|DT_HANDLE
|22|DT_CODE
|23|DT_DATASOURCE
|24|DT_RESOURCE
|25|DT_ANY
|26|DT_COMPRESS
|27|DT_DICTIONARY
|28|DT_DATEHOUR
|29|DT_DATEMINUTE
|30|DT_IP
|31|DT_INT128
|32|DT_OBJECT
