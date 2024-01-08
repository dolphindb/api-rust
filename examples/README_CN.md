# DolphinDB Rust API 使用样例

### 1. 概述

#### 1.1 环境配置

- 安装Rust语言并配置环境变量，构建项目。在api-rust/目录下使用如下指令添加环境变量。请注意，执行export指令只能临时添加环境变量，若需要让变量持久生效，请根据Linux相关教程修改系统文件。

```bash
$ cd api-rust/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/api/src/apic
cargo build
```

- 搭建DolphinDB Server。 详见[DolphinDB 教程](https://github.com/dolphindb/Tutorials_CN/blob/master/single_machine_cluster_deploy.md)。

启动DolphiniDB Server后，请根据本地实际的DolphinDB配置修改样例代码中的节点地址、端口、用户名和密码。本教程使用默认地址“127.0.0.1”，默认端口8848，用户名“admin”， 密码“123456“。

#### 1.2 样例说明

目前有4个Rust API的样例代码，如下表所示，均位于examples目录下：

* [RWMemoryTable.rs](./RWMemoryTable.rs): 内存表的写入和读取操作。
* [RWDFSTable.rs](./RWDFSTable.rs): 分布式表的数据写入。
* [DFSWritingWithMultiThread.rs](./DFSWritingWithMultiThread.rs): 分布式表的多线程并行写入。
* [StreamingDataWriting.rs](./StreamingDataWriting.rs): 流数据写入的样例。

这些例子的开发环境详见[DolphinDB Rust API](https://github.com/dolphindb/api-rust/blob/master/README.md)。

注意运行 DFSWritingWithMultiThread 前需要在 server 执行如下脚本，先创建好对应的分布式表：

```
dbName = "dfs://natlog"
tableName = "natlogrecords"
db1 = database("", VALUE, datehour(2020.03.01T00:00:00)..datehour(2020.12.30T00:00:00) )
db2 = database("", HASH, [IPADDR, 50]) 
db3 = database("", HASH,  [IPADDR, 50]) 
db = database(dbName, COMPO, [db1,db2,db3])
data = table(1:0, ["fwname","filename","source_address","source_port","destination_address","destination_port","nat_source_address","nat_source_port","starttime","stoptime","elapsed_time"], [SYMBOL,STRING,IPADDR,INT,IPADDR,INT,IPADDR,INT,DATETIME,DATETIME,INT])
db.createPartitionedTable(data,tableName,`starttime`source_address`destination_address)
```

运行 StreamingDataWriting 前需要在 server 执行以下脚本创建流数据表：

```DolpinDB
st=streamTable(1000000:0,`id`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cuuid`cip`cint128,[LONG,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,IPADDR,INT128])
enableTableShareAndPersistence(st,"st1",true,false,1000000)
```

```bash
cargo run --example RWMemoryTable
```

#### 1.3 连接到DolphinDB Server

Rust API连接到DolphinDB Server后才可以从Server端读写数据，这需要先声明一个DBConnection对象。并调用`connect`方法建立一个到DolphinDB Server的连接。

```Rust
let conn = DBConnection::new();
conn.connect(HOST, PORT, USER, PASS);
```

下面对每个例子进行简单说明。

### 2. 内存表的读写

#### 2.1 创建内存表

在Rust客户端中对DBConnection类的连接对象调用`run`方法，能够在DolphinDB Server端执行DolphinDB脚本。

```Rust
let script = "kt = keyedTable(`col_int, 2000:0, `col_int`col_short`col_long`col_float`col_double`col_bool`col_string,  [INT, SHORT, LONG, FLOAT, DOUBLE, BOOL, STRING]); ";
conn.run(script);
```

这段脚本会在Server端创建一个名为kt的带主键的内存表，这个表有7列，它们的列类型分别是INT， SHORT， LONG， FLOAT， DOUBLE， BOOL， STRING。

#### 2.2 写入数据

上面在Server端创建了一个内存表，下面在Rust中创建7个列来写入数据。

##### 2.2.1 创建列 

在Rust中使用`create_vector`函数创建对应类型的列。

```Rust
let coltypes: [c_int; 7] = [DT_INT, DT_SHORT, DT_LONG, DT_FLOAT, DT_DOUBLE, DT_BOOL, DT_STRING];
let colnum = 7;
let mut colv: Vec<ddb::Vector> = Vec::new();
for i in 0..(colnum) {
    colv.push(create_vector(coltypes[i], 0));
}
```

##### 2.2.2 准备数据

对应表的7列，在Rust中创建7个数组，并依次向各数组添加数据。

```Rust
const ROWNUM : usize = 2;
let mut v0: [c_int; ROWNUM] = [0; ROWNUM];
let mut v1: [c_short; ROWNUM] = [0; ROWNUM];
let mut v2: [c_longlong; ROWNUM] = [0; ROWNUM];
let mut v3: [c_float; ROWNUM] = [0.0; ROWNUM];
let mut v4: [c_double; ROWNUM] = [0.0; ROWNUM];
let mut v5: [bool; ROWNUM] = [false; ROWNUM];
let mut v6: [&str; ROWNUM] = [""; ROWNUM];

for i in 0..ROWNUM {
    v0[i] = i as c_int;
    v1[i] = 255;
    v2[i] = 10000 + i as c_longlong;
    v3[i] = 133.3;
    v4[i] = 255.0;
    v5[i] = true;
    v6[i] = "str";
}
```

##### 2.2.3 添加数据到列

使用API提供的`append`系列方法将数组追加到各列中。

```Rust
let rows = ROWNUM as i32;
colv[0].append_int(&v0[..], rows);
colv[1].append_short(&v1[..], rows);
colv[2].append_long(&v2[..], rows);
colv[3].append_float(&v3[..], rows);
colv[4].append_double(&v4[..], rows);
colv[5].append_bool(&v5[..], rows);
colv[6].append_string(&v6[..], rows);
```	 

##### 2.2.4 数据写入

使用`run_func`方法运行DolphinDB函数。`run_func`的第一个参数为DolphinDB中的函数名，第二个参数是该函数所需的一个或者多个参数，为Constant类型的向量。如下所示，例子中使用了[tableInsert](https://www.dolphindb.cn/cn/help/index.html?tableInsert.html)函数将各列的数据写入到表kt中,其中`tableInsert{kt}`是一个[tableInsert](https://www.dolphindb.cn/cn/help/index.html?tableInsert.html)的[部分应用](https://www.dolphindb.com/cn/help/PartialApplication.html):
```Rust
let args: [Constant; 7] = [
    colv[0].to_constant(),
    colv[1].to_constant(),
    colv[2].to_constant(),
    colv[3].to_constant(),
    colv[4].to_constant(),
    colv[5].to_constant(),
    colv[6].to_constant(),
];
conn.run_func("tableInsert{kt}", &args[..]);
```

`run_func`的第二个参数是一个Constant类型的数组，可以将多个Constant对象加入其中。其他的诸如Table和Vector类型，需要调用`to_constant`方法转换成Constant类型的对象。

> 请注意: 这里通过脚本使用了DolphinDB的[`tableInsert`](https://www.dolphindb.cn/cn/help/tableInsert.html)函数，对于分区表，`tableInsert`的第二个参数不能如样例中是多个Vector组成的数组，而只能是一个Table。

#### 2.3 读取数据

我们可以通过直接执行DolphinDB SQL查询语句，如select * from kt， 从Server端读取数据，如

```Rust
let res = conn.run("select * from kt");
```

这里我们可以确定返回的是一个Table，对其调用`to_table`方法将其转换为一个Table对象，就可以适用于Table类的各种方法，Table类的更多方法可以参考Rust API的文档。

```Rust
let res_table = res.to_table();
println!("{} {}", res_table.rows(), " rows ");
println!("{} {}", res_table.columns(), " columns ");
```

对于res_table， 可以通过`get_column`的方式获取各列。Table的列是Vector类型，而后对Vector使用`get`方法获取列中单个的值。

```Rust
let mut res_col: Vec<ddb::Vector> = Vec::new();
for i in 0..(colnum) {
    res_col.push(res_table.get_column(i as c_int));
}
let v1 = res_col[0].get(0);
```

Constant以及Table和Vector等类均有`get_string`方法以直观地打印数据。

```Rust
let re1 = conn.run("select  top 5 * from kt");
println!("{}", re1.get_string());
```

另外，通过`run`方法返回的是一个Constant对象，当不确定它的数据形式时，需要通过调用`get_form`方法来判断。
```Rust
let res = conn.run("select * from kt");
let from_number = res.get_form();
```

get_form方法返回数据形式对应的序号，具体的对应规则请参考[Rust API README](https://github.com/dolphindb/api-rust/blob/master/README.md)。

### 3. 分布式表的读写

本例实现了用单线程往分布式数据库读写数据的功能。

#### 3.1 创建分布式表和数据库

在DolphinDB中执行以下脚本创建分布式表和数据库:

```
dbPath='dfs://datedb'; 
if(existsDatabase(dbPath)) dropDatabase(dbPath); 
t = table(100:0, `id`date`x , [INT, DATE, DOUBLE]); 
db=database(dbPath, VALUE, 2017.08.07..2017.08.11); 
tb=db.createPartitionedTable(t, `pt,`date)
```

该分布式表按date(日期)值（VALUE）分区。

#### 3.2 数据写入

在写入数据到分布式表中时，分区字段的值需要在分区范围内，才可以正常写入到分布式表中。例如写入样例中的这个分布式表，需要本地的表中date这一列的值转换为DATE类型后，介于2017.08.07..2017.08.11之间。若不在这个时间范围内，需要按照用户手册中[增加分区](http://www.dolphindb.cn/cn/help/AddPartitions.html)介绍的两种方法增加分区，即将配置参数newValuePartitionPolicy设定为add，或者使用[addValuePartitions](http://www.dolphindb.cn/cn/help/addValuePartitions.html)函数。

##### 3.2.1 准备数据

准备三列数据，类型分别为DT_INT，DT_DATE，DT_DOUBLE。而后通过`create_table_by_vector`函数使用列创建表。

```Rust
const ROWNUM: usize = 1000;

let v1 = create_vector(DT_INT, 0);
let v2 = create_vector(DT_DATE, 0);
let v3 = create_vector(DT_DOUBLE, 0);
for i in 0..ROWNUM {
    v1.append(&create_int(i as c_int));
    v2.append(&create_date(2017, 8, 7 + (i as c_int) % 5));
    v3.append(&create_double(3.1415926));
}
let cols: [Vector; 3] = [v1, v2, v3];
let colnames: [&str; 3] = ["id", "date", "x"];
let t = create_table_by_vector(&colnames[..], &cols[..]);
```

##### 3.2.2 向分布式表写入数据

DolphinDB的分布式表需要使用[loadTable](https://www.dolphindb.cn/cn/help/index.html?loadTable.html)加载后才能进行操作。另外由于目标表是分布式表，参数args不能是内存表样例中的Vector数组， 而必须是一个Table。

```Rust
let args: [Constant; 1] = [t.to_constant()];
conn.run_func("tableInsert{loadTable('dfs://datedb', `pt)}", &args[..]);
```
> 请注意: 单线程写入会有瓶颈，多线程能显著提高DolphinDB的吞吐量和写入性能，建议在实际环境中采用多线程写入数据。

#### 3.3 从分布式数据库中读取数据

和上一节相同，执行SQL语句同样需要使用[loadTable](https://www.dolphindb.cn/cn/help/index.html?loadTable.html)加载数据库中的表。

```Rust
let res = conn.run("select count(*) from loadTable('dfs://datedb', `pt)");
println!("{}", res.get_string());
```

> 请注意: 在分区数量多且数据庞大时，读取操作会较为缓慢。

### 4. 多线程并行写入数据库

本例实现了用多线程往分布式数据库写入数据的功能。为了对比单线程与多线程的写入性能，特用本例子的程序进行了对比测试。测试在一台台式机上完成，CPU为6 核 12 线程的Intel I7，内存32GB ，128GB SSD和2TB 7200RPM HDD 。DolphinDB采用单节点部署,启动Redo Log和写入缓存。元数据和Redo Log写入SSD，数据写入HDD。数据存储启用压缩功能。本例程序与DolphinDB运行在同一台主机上。测试结果如下表：

|线程数|写入批次大小|写入延时（毫秒)|每秒写入记录数|
|--|--|--|--|
|1|10,000|73|65,000|
|5|10,000|108|228,000|
|10|10,000|127|393,000|
|1|100,000|99|505,000|
|5|100,000|136|1,061,000|
|10|100,000|246|1,180,000|

从上表可以验证，批量相同是，多线程相比单线程能显著提高吞吐量和写入性能。需要说明的是，上述测试中数据库采用同步写入确保数据安全，也就是说只有元数据和Redo Log刷入磁盘后，方可返回给客户端。如果采用异步写入，写入的延迟和吞吐量可以进一步改进。

#### 4.1 创建分布式数据库和表

本例数据库用于网络设备的日志监控平台，应用场景要求每秒能写入300万条记录，常用的查询基于时间段、源IP地址和目的IP地址。

DolphinDB采用分区机制，每个分区副本的数据采用列式压缩存储。DolphinDB不提供行级的索引，而是将分区作为数据库的物理索引。一个分区字段相当于给数据表建了一个物理索引。如果查询时用到了该字段做数据过滤，SQL引擎就能快速定位需要的数据块，而无需对整表进行扫描。因此本例数据库基于常用查询的三个字段建立复合分区，第一个维度按时间分区，第二个维度按源IP地址分区，第三个维度按目的IP地址分区。时间维度通常按天、按小时或按月进行值（VALUE）分区，具体选哪个需要根据数据量和典型查询场景进行具体分析和设计。若查询时间段跨度不大、单位时间内采集数据量大，可按小时进行分区，若查询时间段跨度大、单位时间内数据量不大，可按月分区。另外2个维度的分区可以采用哈希、范围、值、列表等多种方法，原则就是要根据业务特点对数据进行均匀分割，让每个分区压缩前的大小控制在100MB左右。但如果数据表为宽表（几百甚至几千个字段），若单个应用只会使用一小部分字段，因为未用到的字段不会加载到内存，不影响查询效率，这种情况可以适当放大分区上限的范围。有关分区设计的原理和详细说明可参阅[分区数据库教程](https://github.com/dolphindb/Tutorials_CN/blob/master/database.md)。本例单位时间写入数据量比较大，故第一个维度按小时分区，第二、第三维度是IP地址，按HASH各分50个区比较合适，建数据库表的代码如下：

```
dbName = "dfs://natlog"
tableName = "natlogrecords"
db1 = database("", VALUE, datehour(2020.03.01T00:00:00)..datehour(2020.12.30T00:00:00) )
db2 = database("", HASH, [IPADDR, 50]) 
db3 = database("", HASH,  [IPADDR, 50]) 
db = database(dbName, COMPO, [db1,db2,db3])
data = table(1:0, ["fwname","filename","source_address","source_port","destination_address","destination_port","nat_source_address","nat_source_port","starttime","stoptime","elapsed_time"], [SYMBOL,STRING,IPADDR,INT,IPADDR,INT,IPADDR,INT,DATETIME,DATETIME,INT])
db.createPartitionedTable(data,tableName,`starttime`source_address`destination_address)
```

#### 4.2 多线程写入数据

在写入数据时要注意的是，并行写入时，多个线程不能同时往DolphinDB分布式数据库的同一个分区写数据，所以产生数据时，要保证每个线程写入的数据是属于不同分区的。

本例通过为每个写入线程平均分配分区的方法（比如10个线程，50个分区，则线程1写入1-5，线程2写入6-10，其他线程依次类推），保证多个写入线程写到不同的分区。其中每个IP地址的hash值是通过API内置的`get_hash`计算的：得到相同的hash值，说明数据属于相同分区，反之属于不同分区。

注意：若分布式数据库不是HASH分区，可以通过如下方式确保不同的线程写不同的分区：

* 若采用了范围（RANGE）分区，可以先在server端执行函数schema(database(dbName)).partitionSchema[1]获取到分区字段的分区边界（partitionSchema取第一个元素的前提是一般数据库采用两层分区，第一层是日期，第二层是设备或股票进行范围分区）。然后对比数据的分区字段的取值和分区的边界值，控制不同的线程负责不同的1个或多个分区。

* 对于分区类型为值（VALUE）分区、列表（LIST）分区，用值比较的方法可以判定数据所属的分区。然后不同的线程负责写1个或多个不同分区。

例如，本例`create_demo_table`函数中的这一段代码通过`get_hash`方法，对buckets取余获取适合分区的IP值。

```Rust
let spIP = create_constant(DT_IP);
for j in 1..255 {
    sip[0] = j as c_uchar;
    spIP.set_binary(&sip[..]);
    let x = spIP.get_hash(50) as c_uchar;
    if x >= startp && x < startp + pcount {
        break;
    }
}
```

多线程写入的具体流程是：用`thread::spawn`装载函数开启多线程，将要写入的数据准备好，然后对每个节点都获取一个连接，以多线程并发写入。create_demo_table函数可以参考样例代码。

多线程写入示例:

```Rust
static HOSTS: [&str; 5] = [ "192.168.1.135",  
              "192.168.1.135", "192.168.1.135",
              "192.168.1.135", "192.168.1.135"];
static PORTS: [c_int; 5] = [1621, 1622, 1623, 1624, 1625];

fn finsert(rows: c_int, startp: c_uchar, pcount: c_uchar, starttime: c_int,
           timeInc: c_int, p: c_int, inserttimes: c_int) {
    let conn = DBConnection::new();
    let success = conn.connect(&HOSTS[p as usize], PORTS[p as usize], USER, PASS);
    if !success {
        panic!("connect failed");
    }
    let t = create_demo_table(rows, startp, pcount, starttime, timeInc);
    let args: [Constant; 1] = [t.to_constant()];
    for i in 0..inserttimes {
        conn.run_func(
            "tableInsert{loadTable('dfs://natlog', `natlogrecords)}",
            &args[..],
        );
    }
}
fn main() {
    let thread_count = 5;
    let mut thread_handlers = Vec::new();
    let tablerows = 1000;
    let inserttimes = 10;
    for i in 0..thread_count {
        thread_handlers.push(thread::spawn(move || {
            finsert( tablerows,  (i * 5 - 1) as c_uchar,
                (5) as c_uchar, (get_epoch_time() / 1000) as c_int,
                (i * 5) as c_int, i as c_int,
                inserttimes as c_int,
            );
        }))
    }
    for handler in thread_handlers {
        handler.join();
    }
}
```

示例中提供了多个host和port，也即多个DolphinDB Server节点，每个连接对应一个并发线程。通过多个节点写入可以使各节点负载均衡，提高写入效率。样例代码最多支持10个线程同时写入。

### 5. 流数据写入和订阅

使用流数据需要配置发布节点和订阅节点，详见
[DolphinDB 流数据教程](https://github.com/dolphindb/Tutorials_CN/blob/master/streaming_tutorial.md)。

#### 5.1 流数据写入

##### 5.1.1 创建流表

首先在DolphinDB Server端的流数据发布节点上执行以下脚本创建流表：

```DolpinDB
st=streamTable(1000000:0,`id`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cuuid`cip`cint128,[LONG,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,IPADDR,INT128])
enableTableShareAndPersistence(st,"st1",true,false,1000000)
```

##### 5.1.2 写数据

在DolphinDB Server端创建流表后，在Rust中写入数据到流表。写入流程与写入内存表基本相同，创建要写入的表，连接节点，写入数据。

因为该流表是共享表，所以写入时，在tableInsert中使用[objByName](https://www.dolphindb.cn/cn/help/index.html?objByName.html)，即可获取到该流表。

```Rust
conn.run_func("tableInsert{objByName(`st1)}", &args[..]);
```
