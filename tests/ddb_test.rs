extern crate ddb;
extern crate libc;
use crate::ddb::Defobj;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

static test_host: &str = "localhost";
static test_port: libc::c_int = 8848;
static test_user: &str = "admin";
static test_pass: &str = "123456";

#[test]
fn DBConnection_test() {
    let conn = ddb::DBConnection::new();
    assert!(conn.connect(test_host, test_port, test_user, test_pass));
    let x = conn.run("1+1");
    assert_eq!(x.get_int(), 2);
    conn.upload("two", x);
    let y = conn.run("two");
    assert_eq!(y.get_int(), 2);
    conn.close();
}

#[test]
fn Constant_Defobj() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    let x = conn.run("1+1");
    assert_eq!(x.get_int(), 2);
    assert_eq!(x.get_string(), "2");
    assert_eq!(x.get_short(), 2);
    assert_eq!(x.get_long(), 2);
    assert_eq!(x.get_float(), 2.0);
    assert_eq!(x.get_double(), 2.0);

    assert_eq!(x.get_type(), ddb::DT_INT);
    assert_eq!(x.is_scalar(), true);
    assert_eq!(x.is_dictionary(), false);
    assert_eq!(x.is_pair(), false);
    assert_eq!(x.is_vector(), false);
    assert_eq!(x.is_set(), false);
    assert_eq!(x.is_matrix(), false);
    assert_eq!(x.is_table(), false);
    assert_eq!(x.is_large_constant(), false);
    assert_eq!(x.get_form(), 0);

    x.set_int(1);
    assert_eq!(x.get_string(), "1");
    x.set_short(2);
    assert_eq!(x.get_string(), "2");
    x.set_long(3);
    assert_eq!(x.get_string(), "3");
    x.set_float(4.0);
    assert_eq!(x.get_string(), "4");
    x.set_double(5.0);
    assert_eq!(x.get_string(), "5");
    x.set_bool(false);
    assert_eq!(x.get_bool(), false);
    let y = ddb::create_string("123123");
    y.set_string("112312321321");
    assert_eq!(y.get_string(), "112312321321");
    x.set_null();
    assert!(x.is_null());
    conn.close();
}

#[test]
fn Constant_create() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    let cbool = ddb::create_bool(true);
    assert_eq!(cbool.get_bool(), true);
    let cint = ddb::create_int(2147483647);
    assert_eq!(cint.get_int(), 2147483647);
    let cshort = ddb::create_short(32767);
    assert_eq!(cshort.get_short(), 32767);
    let clong = ddb::create_long(100000000000);
    assert_eq!(clong.get_long(), 100000000000);
    let cfloat = ddb::create_float(1.732);
    assert_eq!(cfloat.get_float(), 1.732);
    let cdouble = ddb::create_double(3.1415926);
    assert_eq!(cdouble.get_double(), 3.1415926);
    let cstr = ddb::create_string("hello world");
    assert_eq!(cstr.get_string(), "hello world");
    conn.close();
    //   close
    // upload
}

#[test]
fn Constant_setByIndex() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);

    let v1 = ddb::create_vector(ddb::DT_INT, 3);
    v1.set_int_by_index(0, -2147483648);
    assert_eq!(v1.get(0).get_int(), -2147483648);
    ddb::del_constant(v1.to_constant());

    let v2 = ddb::create_vector(ddb::DT_BOOL, 3);
    v2.set_bool_by_index(1, true);
    assert_eq!(v2.get(1).get_bool(), true);
    ddb::del_constant(v2.to_constant());

    let v3 = ddb::create_vector(ddb::DT_SHORT, 3);
    v3.set_short_by_index(2, -32768);
    assert_eq!(v3.get(2).get_short(), -32768);
    ddb::del_constant(v3.to_constant());

    let v4 = ddb::create_vector(ddb::DT_LONG, 3);
    v4.set_long_by_index(0, 0);
    assert_eq!(v4.get(0).get_long(), 0);
    ddb::del_constant(v4.to_constant());

    let v5 = ddb::create_vector(ddb::DT_FLOAT, 3);
    v5.set_float_by_index(0, -2.236);
    assert_eq!(v5.get(0).get_float(), -2.236);
    ddb::del_constant(v5.to_constant());

    let v6 = ddb::create_vector(ddb::DT_DOUBLE, 3);
    v6.set_double_by_index(1, -3.141592);
    assert_eq!(v6.get(1).get_double(), -3.141592);
    ddb::del_constant(v6.to_constant());

    let v7 = ddb::create_vector(ddb::DT_STRING, 3);
    v7.set_string_by_index(2, "teststr");
    assert_eq!(v7.get(2).get_string(), "teststr");
    ddb::del_constant(v7.to_constant());

    let v8 = ddb::create_vector(ddb::DT_ANY, 3);
    v8.set_null_by_index(0);
    assert_eq!(v8.get(0).is_null(), true);
    ddb::del_constant(v8.to_constant());

    conn.close();
}

#[test]
fn Constant_setsilce() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);

    let v1 = ddb::create_vector(ddb::DT_INT, 5);
    let s1: [c_int; 3] = [1, 1, 1];
    v1.set_int_slice(0, 3, &s1[..]);
    assert_eq!(v1.get(0).get_int(), 1);
    ddb::del_constant(v1.to_constant());

    let v2 = ddb::create_vector(ddb::DT_BOOL, 10);
    let s2: [bool; 3] = [true, true, true];
    let s2x: [bool; 3] = [false, false, false];
    v2.set_bool_slice(0, 3, &s2[..]);
    v2.set_bool_slice(2, 3, &s2x[..]);
    assert_eq!(v2.get(2).get_bool(), false);
    ddb::del_constant(v2.to_constant());

    let v3 = ddb::create_vector(ddb::DT_SHORT, 5);
    let s3: [c_short; 3] = [-1, -1, -1];
    v3.set_short_slice(1, 3, &s3[..]);
    assert_eq!(v3.get(3).get_short(), -1);
    ddb::del_constant(v3.to_constant());

    let v4 = ddb::create_vector(ddb::DT_LONG, 5);
    let s4: [c_longlong; 3] = [100000000000, 100000000000, 100000000000];
    v4.set_long_slice(0, 3, &s4[..]);
    assert_eq!(v4.get(0).get_long(), 100000000000);
    ddb::del_constant(v4.to_constant());

    let v5 = ddb::create_vector(ddb::DT_FLOAT, 10);
    let s5: [c_float; 3] = [1.7, 1.8, 1.9];
    v5.set_float_slice(0, 3, &s5[..]);
    assert_eq!(v5.get(2).get_float(), 1.9);
    ddb::del_constant(v5.to_constant());

    let v6 = ddb::create_vector(ddb::DT_DOUBLE, 3);
    let s6: [c_double; 3] = [1.11111, 2.33333, 3.33333];
    v6.set_double_slice(0, 3, &s6[..]);
    assert_eq!(v6.get(1).get_double(), 2.33333);
    ddb::del_constant(v6.to_constant());

    let v7 = ddb::create_vector(ddb::DT_STRING, 5);
    let s7: [&str; 3] = ["first", "second", "third"];
    v7.set_string_slice(0, 3, &s7[..]);
    assert_eq!(v7.get(0).get_string(), "first");
    ddb::del_constant(v7.to_constant());

    conn.close();
}

#[test]
fn Vector_append() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);

    let v1 = ddb::create_vector(ddb::DT_INT, 0);
    let s1: [c_int; 3] = [1, 1, 1];
    v1.append_int(&s1[..], 3);
    assert_eq!(v1.get(0).get_int(), 1);
    assert_eq!(v1.size(), 3);
    ddb::del_constant(v1.to_constant());

    let v2 = ddb::create_vector(ddb::DT_BOOL, 0);
    let s2: [bool; 3] = [true, true, true];
    let s2x: [bool; 3] = [false, false, false];
    v2.append_bool(&s2[..], 3);
    v2.append_bool(&s2x[..], 2);
    assert_eq!(v2.get(3).get_bool(), false);
    assert_eq!(v2.size(), 5);
    ddb::del_constant(v2.to_constant());

    let v3 = ddb::create_vector(ddb::DT_SHORT, 0);
    let s3: [c_short; 3] = [-1, -1, -1];
    v3.append_short(&s3[..], 3);
    assert_eq!(v3.get(2).get_short(), -1);
    ddb::del_constant(v3.to_constant());

    let v4 = ddb::create_vector(ddb::DT_LONG, 0);
    let s4: [c_longlong; 3] = [100000000000, 100000000000, 100000000000];
    v4.append_long(&s4[..], 2);
    assert_eq!(v4.get(0).get_long(), 100000000000);
    assert_eq!(v4.size(), 2);
    ddb::del_constant(v4.to_constant());

    let v5 = ddb::create_vector(ddb::DT_FLOAT, 10);
    let s5: [c_float; 3] = [1.7, 1.8, 1.9];
    v5.append_float(&s5[..], 3);
    assert_eq!(v5.get(12).get_float(), 1.9);
    ddb::del_constant(v5.to_constant());

    let v6 = ddb::create_vector(ddb::DT_DOUBLE, 3);
    let s6: [c_double; 3] = [1.11111, 2.33333, 3.33333];
    v6.append_double(&s6[..], 3);
    assert_eq!(v6.get(4).get_double(), 2.33333);
    ddb::del_constant(v6.to_constant());

    let v7 = ddb::create_vector(ddb::DT_STRING, 0);
    let s7: [&str; 3] = ["first", "second", "third"];
    v7.append_string(&s7[..], 2);
    assert_eq!(v7.get(1).get_string(), "second");
    ddb::del_constant(v7.to_constant());

    conn.close();
}

#[test]
fn Set_test() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    let s1 = ddb::create_set(ddb::DT_INT, 10);
    let c1 = conn.run("set( 3 5 7)");
    let c2 = conn.run("set(4 3 )");

    assert!(c1.is_set());
    let s = c1.to_set();
    assert_eq!(s.size(), 3);
    //    let one = ddb::create_int(1);
    //     s.append(&mut one);
    //    assert_eq!(one.get_string(),"");
    let one = ddb::create_int(1);
    s.append(one);
    assert_eq!(s.size(), 4);

    assert!(s.is_super_set(ddb::create_int(3)));
    assert_eq!(s.interaction(ddb::create_int(3)).get_string(), "set(3)");
    //   println!("{}", c2.get_string());
    //   let p = Constant::new();
    //    s.contain(ddb::create_int(3), &c2);
    //  assert!(s.inverse(c2.to_constant()));

    // assert_eq!(s.get_string(),"");
    assert_eq!(s.get_script(), "set()");
    assert!(s.is_large_constant());
    assert_eq!(s.get_sub_vector(1, 1).get_string(), "[3]");
    s.clear();
    assert_eq!(s.size(), 0);
}

#[test]
fn Vector_test() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);

    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    assert_eq!(v1.get(0).get_string(), "3");
    v1.remove(1);
    assert_eq!(v1.size(), 4);
    //assert_eq!(v1.get_string(),"3");
    v1.append(&ddb::create_int(8));
    assert_eq!(v1.get(4).get_string(), "8");
    v1.set_name("vector1");
    assert_eq!(v1.get_name(), "vector1");
    assert_eq!(v1.get_column_label().get_string(), "vector1");
    assert_eq!(v1.is_view(), false);
    assert_eq!(v1.get_capacity(), 5);
    v1.reserve(10);
    assert_eq!(v1.get_capacity(), 10);
    v1.reserve(5);
    assert_eq!(v1.get_unit_length(), 4);
    //assert_eq!(v1.get(0).get_string(),"8");
    assert_eq!(v1.size(), 5);
    //v1.remove_by_index(ddb::create_int(3));
    //assert_eq!(v1.size(),4);
    assert_eq!(v1.get_instance(3).size(), 3);
    let v2 = v1.get_sub_vector(0, 2);
    assert_eq!(v2.get_string(), "[3,6]");
    v1.fill(1, 2, ddb::create_int(7));
    assert_eq!(v1.get(2).get_int(), 7);

    v2.next(1);
    assert_eq!(v2.get_string(), "[6,]");
    v2.prev(1);
    assert_eq!(v2.get_string(), "[,6]");
    v1.reverse();
    assert_eq!(v1.get(0).get_int(), 8);
    v1.replace(ddb::create_int(7), ddb::create_int(4));
    assert_eq!(v1.get(3).get_int(), 4);
    //  assert!(v1.valid_index(0));
    v1.add_index(0, 2, 1);
    assert_eq!(v1.get(0).get_int(), 9);
    v1.neg();
    assert_eq!(v1.get(0).get_int(), -9);

    let ca = conn.run("3 6 1 5 9");
    let va = c1.to_vector();
    va.initialize();

    let cb = conn.run("3 6 1 5 9");
    let vb = c1.to_vector();
    vb.clear();
    //   assert_eq!(va.size(),1);
    //  assert_eq!(vb.size(),0);
}

#[test]
fn Table_test() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);

    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();
    t1.set_name("table1");
    assert_eq!(t1.get_name(), "table1");
    assert_eq!(t1.get_column_name(0), "a");
    let v1 = t1.get_column(0);
    assert_eq!(v1.get_name(), "a");
    let v2 = t1.get_column_by_name("a");
    assert_eq!(v1.get_string(), v2.get_string());
    assert_eq!(t1.columns(), 3);
    assert_eq!(t1.rows(), 3);
    assert_eq!(t1.get_column_type(0), ddb::DT_INT);
    assert_eq!(t1.get_script(), "table1");
    //  assert_eq!(t1.get_column_qualifier(1),"tab");
    t1.set_column_name(0, "a1");
    assert_eq!(t1.get_column_name(0), "a1");
    t1.set_column_name(0, "a");
    assert_eq!(t1.get_column_index("b"), 1);
    assert_eq!(t1.contain("c"), true);
    let t2 = t1.get_value();
    assert_eq!(t1.get_string(), t2.get_string());
    assert!(t1.sizeable());
    assert_eq!(t1.get_string_by_index(0), " 1 x 10.8");
    let t3 = t1.get_window(0, 2, 0, 2).to_table();
    assert_eq!(t3.columns(), 2);
    assert_eq!(t1.keys().get_string(), "[\"a\",\"b\",\"c\"]");
    assert_eq!(
        t1.get_member(ddb::create_string("a")).get_string(),
        "[1,2,3]"
    );
    assert_eq!(
        t1.get_member(ddb::create_string("a")).get_string(),
        "[1,2,3]"
    );
    // assert_eq!(t1.get_instance(3).get_string(),"");
}
/*
  #[test]
  fn Matrix_test() {
          let host = "localhost";
        let user = "admin";
        let pass = "123456";
       let conn= ddb::DBConnection::new();
        conn.connect(host, 1621, user, pass);
       let m1 = conn.run("1..10$5:2").to_Matrix();
        assert_eq!(m1.size(),10);
      //  let label1 = ddb::create_vector(ddb::DT_STRING, 0);
      //  label1.append(ddb::create_String("a"));
      //  label1.append(ddb::create_String("b"));
      //  m1.set_row_label(label1.to_constant());
    //    assert_eq!(m1.get_string(),"");
      // m1.reshape(5,2);
      let v1 = m1.get_column(1);
     //  assert_eq!(v1.size(),2);



        conn.close();
    }
*/

#[test]
fn Dictionary_test() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    let d1 = conn
        .run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y);")
        .to_dictionary();
    assert_eq!(d1.count(), 3);
    assert_eq!(d1.get_member(ddb::create_int(1)).get_double(), 6.4);
    assert_eq!(d1.get_key_type(), ddb::DT_SHORT);
    //     assert_eq!(d1.keys().get_string(),"[1,2,3]");
    //   assert_eq!(d1.values().get_string(),"[6.4,4.6,5.3]");
    assert_eq!(d1.get_script(), "dict()");
    d1.remove(ddb::create_short(1));
    assert_eq!(d1.count(), 2);
    d1.set(ddb::create_short(1), ddb::create_float(6.4));
    assert_eq!(d1.count(), 3);
    //     d1.set_by_string("1", ddb::create_float(6.4));
    //   assert_eq!(d1.get_cell(2,2).get_string(),"");
    //   assert_eq!(d1.get_member_by_string("2").get_double(),6.4);
    //  let label1 = ddb::create_vector(ddb::DT_STRING, 0);
    //  label1.append(ddb::create_String("a"));
    //  label1.append(ddb::create_String("b"));
    //  m1.set_row_label(label1.to_constant());
    //    assert_eq!(m1.get_string(),"");
    // m1.reshape(5,2);
    // let v1 = m1.get_column(1);
    //  assert_eq!(v1.size(),2);

    conn.close();
}

#[test]
fn In_memory_table() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    conn.run("t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);share t as tglobal;");

    let ta = CreateDemoTable();

    let args: [ddb::Constant; 1] = [ta.to_constant()];
    conn.run_func("tableInsert{tglobal}", &args[..]);

    let result = conn.run("select * from tglobal");
    let tc = result.to_table();
    assert_eq!(tc.get_column_name(0), "name");
    assert_eq!((tc.columns() > 0), true);
}

#[test]
fn Disk_Table() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    conn.run("t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);db=database('./demoDB');saveTable(db, t, `dt);share t as tDiskGlobal;");

    let ta = CreateDemoTable();
    let args: [ddb::Constant; 1] = [ta.to_constant()];
    conn.run_func("tableInsert{tDiskGlobal}", &args[..]);
    conn.run("saveTable(database('./demoDB'),tDiskGlobal,`dt)");

    let result = conn.run("select * from tDiskGlobal");
    let tc = result.to_table();
    assert_eq!(tc.get_column_name(0), "name");
    assert_eq!((tc.columns() > 0), true);
}

#[test]
fn Partitioned_Table() {
    let conn = ddb::DBConnection::new();
    conn.connect(test_host, test_port, test_user, test_pass);
    conn.run("login(`admin, `123456);dbPath = 'dfs://demoDB';
    if(existsDatabase(dbPath)){dropDatabase(dbPath)};
    tablename = `demoTable;
    db = database(dbPath, VALUE, 1970.01.01..1970.01.03);
    pt=db.createPartitionedTable(table(100:0, `name`date`price, [STRING,DATE,DOUBLE]), tablename, `date);");

    let ta = CreateDemoTable();
    let args: [ddb::Constant; 1] = [ta.to_constant()];
    conn.run_func(
        "tableInsert{loadTable('dfs://demoDB', `demoTable)}",
        &args[..],
    );

    let result = conn.run("select * from loadTable('dfs://demoDB', `demoTable)");
    let tc = result.to_table();
    assert_eq!(tc.get_column_name(0), "name");
    assert_eq!((tc.columns() > 0), true);
}

fn CreateDemoTable() -> ddb::Table {
    let col_names: [&str; 3] = ["name", "date", "price"];
    let coltypes: [c_int; 3] = [ddb::DT_STRING, ddb::DT_DATE, ddb::DT_DOUBLE];
    const rowNum: c_int = 10;
    const colNum: c_int = 3;
    let indexCapacity = 11;
    let ta = ddb::create_table(&col_names[..], &coltypes[..], rowNum, indexCapacity);
    let mut columnVecs: Vec<ddb::Vector> = Vec::new();
    for i in 0..(colNum) {
        columnVecs.push(ta.get_column(i));
    }
    let mut arr2: [c_int; 10] = [1; rowNum as usize];
    let mut arr3: [c_double; 10] = [1.0; rowNum as usize];
    let mut arr1: [&str; 10] = ["asd"; rowNum as usize];

    columnVecs[0].set_string_slice(0, rowNum, &arr1[..]);
    columnVecs[1].set_int_slice(0, rowNum, &arr2[..]);
    columnVecs[2].set_double_slice(0, rowNum, &arr3[..]);
    println!("{}", ta.get_string());
    return ta;
}
