extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

static TEST_HOST: &str = "127.0.0.1";
static TEST_PORT: libc::c_int = 28848;
static TEST_USER: &str = "admin";
static TEST_PASS: &str = "123456";

#[test]
fn Table_set_get_name() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    t1.set_name("table1");
    assert_eq!(t1.get_name(), "table1");

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_get_column_name() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    assert_eq!(t1.get_column_name(0), "a");

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_get_column() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    let v1 = t1.get_column(0);
    assert_eq!(v1.get_name(), "a");

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_get_column_by_name() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    let v2 = t1.get_column_by_name("a");
    assert_eq!(v2.get_name(), "a");

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_columns() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    assert_eq!(t1.columns(), 3);

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_rows() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    assert_eq!(t1.rows(), 3);

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_get_column_type() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    assert_eq!(t1.get_column_type(0), ddb::DT_INT);

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_set_column_name() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    t1.set_column_name(0, "a1");
    assert_eq!(t1.get_column_name(0), "a1");

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_get_column_index() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    assert_eq!(t1.get_column_index("b"), 1);

    conn.close();
    del_constant(t1.to_constant());
}

#[test]
fn Table_get_contain() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
    let t1 = c1.to_table();

    assert_eq!(t1.contain("c"), true);

    conn.close();
    del_constant(t1.to_constant());
}
