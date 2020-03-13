extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

static TEST_HOST: &str = "localhost";
static TEST_PORT: libc::c_int = 8848;
static TEST_USER: &str = "admin";
static TEST_PASS: &str = "123456";

#[test]
fn Set_append() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);

    let c1 = conn.run("set( 3 5 7)");
    let s = c1.to_set();
    let one = ddb::create_int(1);
    s.append(one);
    assert_eq!(s.size(), 4);

    conn.close();
    del_constant(s.to_constant());
}

#[test]
fn Set_remove() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);

    let c1 = conn.run("set( 3 5 7)");
    let s = c1.to_set();
    let x = ddb::create_int(3);
    s.remove(x);
    assert_eq!(s.size(), 2);

    conn.close();
    del_constant(s.to_constant());
}

#[test]
fn Set_is_super_set() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("set( 3 5 7)");
    let s = c1.to_set();

    assert!(s.is_super_set(ddb::create_int(3)));

    conn.close();
    del_constant(s.to_constant());
}

#[test]
fn Set_interaction() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("set( 3 5 7)");
    let s = c1.to_set();

    assert_eq!(s.interaction(ddb::create_int(3)).get_string(), "set(3)");

    conn.close();
    del_constant(s.to_constant());
}

#[test]
fn Set_get_sub_vector() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("set( 3 5 7)");
    let s = c1.to_set();

    assert_eq!(s.get_sub_vector(0, 1).get_string(), "[3]");

    conn.close();
    del_constant(s.to_constant());
}

#[test]
fn Set_clear() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("set( 3 5 7)");
    let s = c1.to_set();

    s.clear();
    assert_eq!(s.size(), 0);

    conn.close();
    del_constant(s.to_constant());
}
