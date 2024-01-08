extern crate ddb;
extern crate libc;
use crate::ddb::*;
//use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

static TEST_HOST: &str = "127.0.0.1";
static TEST_PORT: libc::c_int = 28848;
static TEST_USER: &str = "admin";
static TEST_PASS: &str = "123456";

#[test]
fn Dictionary_count() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let res = conn.run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y);");
    let d1 = res.to_dictionary();

    assert_eq!(d1.count(), 3);

    conn.close();
    del_constant(d1.to_constant());
}

#[test]
fn Dictionary_get_member() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let res = conn.run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y);");
    let d1 = res.to_dictionary();

    assert_eq!(d1.get_member(ddb::create_int(1)).get_double(), 6.4);

    conn.close();
    del_constant(d1.to_constant());
}

#[test]
fn Dictionary_key_type() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let res = conn.run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y);");
    let d1 = res.to_dictionary();

    assert_eq!(d1.get_key_type(), ddb::DT_SHORT);

    conn.close();
    del_constant(d1.to_constant());
}

#[test]
fn Dictionary_remove() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let res = conn.run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y);");
    let d1 = res.to_dictionary();

    d1.remove(ddb::create_short(1));
    assert_eq!(d1.count(), 2);

    conn.close();
    del_constant(d1.to_constant());
}

#[test]
fn Dictionary_set() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let res = conn.run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y);");
    let d1 = res.to_dictionary();

    d1.set(ddb::create_short(5), ddb::create_float(6.4));
    assert_eq!(d1.count(), 4);
    conn.close();
    del_constant(d1.to_constant());
}
