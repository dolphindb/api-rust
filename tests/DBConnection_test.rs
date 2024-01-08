extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

static TEST_HOST: &str = "127.0.0.1";
static TEST_PORT: libc::c_int = 8848;
static TEST_USER: &str = "admin";
static TEST_PASS: &str = "123456";

#[test]
fn DBConnection_connect() {
    let connx = ddb::DBConnection::new();
    assert!(connx.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS));

    connx.close();
}

#[test]
fn DBConnection_run() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let x = conn.run("1+1");
    assert_eq!(x.get_int(), 2);
    conn.upload("two", x);
    let y = conn.run("two");
    assert_eq!(y.get_int(), 2);
    conn.close();
}

#[test]
fn DBConnection_upload() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let x = ddb::create_int(2);
    conn.upload("two", x);
    let y = conn.run("two");
    assert_eq!(y.get_int(), 2);
    conn.close();
}

#[test]
fn DBConnection_runfunc() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);

    conn.run("x = [1,3,5]");
    let a2: [c_int; 3] = [9, 8, 7];

    let y = create_vector(DT_INT, 3);
    y.set_int_slice(0, 3, &a2[..]);

    let args: [Constant; 1] = [y.to_constant()];
    let res = conn.run_func("add{x,}", &args[..]);
    assert!(res.is_vector());

    conn.close();
}
