extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

#[test]
fn Constant_get_int() {
    let x = create_int(2);
    assert_eq!(x.get_int(), 2);
}

#[test]
fn Constant_get_string() {
    let x = create_int(2);
    assert_eq!(x.get_string(), "2");
}
#[test]
fn Constant_get_short() {
    let x = create_int(2);
    assert_eq!(x.get_short(), 2);
}

#[test]
fn Constant_get_long() {
    let x = create_int(2);
    assert_eq!(x.get_long(), 2);
}

#[test]
fn Constant_get_float() {
    let x = create_int(2);
    assert_eq!(x.get_float(), 2.0);
}

#[test]
fn Constant_get_double() {
    let x = create_int(2);
    assert_eq!(x.get_double(), 2.0);
}

#[test]
fn Constant_get_type() {
    let x = create_int(2);
    assert_eq!(x.get_type(), ddb::DT_INT);
}

#[test]
fn Constant_is_scalar() {
    let x = create_int(2);
    assert_eq!(x.is_scalar(), true);
}

#[test]
fn Constant_is_dictionary() {
    let x = create_int(2);
    assert_eq!(x.is_dictionary(), false);
}

#[test]
fn Constant_is_pair() {
    let x = create_int(2);
    assert_eq!(x.is_pair(), false);
}

#[test]
fn Constant_is_vector() {
    let x = create_int(2);
    assert_eq!(x.is_vector(), false);
}

#[test]
fn Constant_is_set() {
    let x = create_int(2);
    assert_eq!(x.is_set(), false);
}

#[test]
fn Constant_is_matrix() {
    let x = create_int(2);
    assert_eq!(x.is_matrix(), false);
}

#[test]
fn Constant_is_table() {
    let x = create_int(2);
    assert_eq!(x.is_table(), false);
}

#[test]
fn Constant_is_large_constant() {
    let x = create_int(2);
    assert_eq!(x.is_large_constant(), false);
}

#[test]
fn Constant_get_form() {
    let x = create_int(2);
    assert_eq!(x.get_form(), 0);
}

#[test]
fn Constant_set_int() {
    let x = create_int(2);
    x.set_int(1);
    assert_eq!(x.get_string(), "1");
}

#[test]
fn Constant_set_long() {
    let x = create_int(2);
    x.set_long(3);
    assert_eq!(x.get_string(), "3");
}

#[test]
fn Constant_set_short() {
    let x = create_int(2);
    x.set_short(3);
    assert_eq!(x.get_string(), "3");
}

#[test]
fn Constant_set_float() {
    let x = create_int(2);
    x.set_float(4.0);
    assert_eq!(x.get_string(), "4");
}

#[test]
fn Constant_set_double() {
    let x = create_int(2);
    x.set_double(5.0);
    assert_eq!(x.get_string(), "5");
}

#[test]
fn Constant_set_bool() {
    let x = create_int(2);
    x.set_bool(false);
    assert_eq!(x.get_bool(), false);
}

#[test]
fn Constant_set_string() {
    let y = ddb::create_string("123123");
    y.set_string("112312321321");
    assert_eq!(y.get_string(), "112312321321");
}

#[test]
fn Constant_set_null() {
    let x = create_int(2);
    x.set_null();
    assert!(x.is_null());
}

#[test]
fn Constant_create_bool() {
    let cbool = ddb::create_bool(true);
    assert_eq!(cbool.get_bool(), true);
}

#[test]
fn Constant_create_int() {
    let cint = ddb::create_int(2147483647);
    assert_eq!(cint.get_int(), 2147483647);
}

#[test]
fn Constant_create_short() {
    let cshort = ddb::create_short(32767);
    assert_eq!(cshort.get_short(), 32767);
}

#[test]
fn Constant_create_long() {
    let clong = ddb::create_long(100000000000);
    assert_eq!(clong.get_long(), 100000000000);
}

#[test]
fn Constant_create_float() {
    let cfloat = ddb::create_float(1.732);
    assert_eq!(cfloat.get_float(), 1.732);
}

#[test]
fn Constant_create_double() {
    let cdouble = ddb::create_double(3.1415926);
    assert_eq!(cdouble.get_double(), 3.1415926);
}

#[test]
fn Constant_create_string() {
    let cstr = ddb::create_string("hello world");
    assert_eq!(cstr.get_string(), "hello world");
}
