extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_char, c_double, c_float, c_int, c_long, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {}

static TEST_HOST: &str = "127.0.0.1";
static TEST_PORT: libc::c_int = 28848;
static TEST_USER: &str = "admin";
static TEST_PASS: &str = "123456";

#[test]
fn Vector_set_int_by_index() {
    let v1 = ddb::create_vector(ddb::DT_INT, 3);
    v1.set_int_by_index(0, -2147483648);
    assert_eq!(v1.get(0).get_int(), -2147483648);
    ddb::del_constant(v1.to_constant());
}

#[test]
fn Vector_set_bool_by_index() {
    let v2 = ddb::create_vector(ddb::DT_BOOL, 3);
    v2.set_bool_by_index(1, true);
    assert_eq!(v2.get(1).get_bool(), true);
    ddb::del_constant(v2.to_constant());
}

#[test]
fn Vector_set_short_by_index() {
    let v3 = ddb::create_vector(ddb::DT_SHORT, 3);
    v3.set_short_by_index(2, -32768);
    assert_eq!(v3.get(2).get_short(), -32768);
    ddb::del_constant(v3.to_constant());
}

#[test]
fn Vector_set_long_by_index() {
    let v4 = ddb::create_vector(ddb::DT_LONG, 3);
    v4.set_long_by_index(0, 0);
    assert_eq!(v4.get(0).get_long(), 0);
    ddb::del_constant(v4.to_constant());
}

#[test]
fn Vector_set_float_by_index() {
    let v5 = ddb::create_vector(ddb::DT_FLOAT, 3);
    v5.set_float_by_index(0, -2.236);
    assert_eq!(v5.get(0).get_float(), -2.236);
    ddb::del_constant(v5.to_constant());
}

#[test]
fn Vector_set_double_by_index() {
    let v6 = ddb::create_vector(ddb::DT_DOUBLE, 3);
    v6.set_double_by_index(1, -3.141592);
    assert_eq!(v6.get(1).get_double(), -3.141592);
    ddb::del_constant(v6.to_constant());
}

#[test]
fn Vector_set_string_by_index() {
    let v7 = ddb::create_vector(ddb::DT_STRING, 3);
    v7.set_string_by_index(2, "teststr");
    assert_eq!(v7.get(2).get_string(), "teststr");
    ddb::del_constant(v7.to_constant());
}

#[test]
fn Vector_set_null_by_index() {
    let v8 = ddb::create_vector(ddb::DT_ANY, 3);
    v8.set_null_by_index(0);
    assert_eq!(v8.get(0).is_null(), true);
    ddb::del_constant(v8.to_constant());
}

#[test]
fn Vector_set_int_slice() {
    let v1 = ddb::create_vector(ddb::DT_INT, 5);
    let s1: [c_int; 3] = [1, 1, 1];
    v1.set_int_slice(0, 3, &s1[..]);
    assert_eq!(v1.get(0).get_int(), 1);
    ddb::del_constant(v1.to_constant());
}

#[test]
fn Vector_set_bool_slice() {
    let v2 = ddb::create_vector(ddb::DT_BOOL, 10);
    let s2: [bool; 3] = [true, true, true];
    let s2x: [bool; 3] = [false, false, false];
    v2.set_bool_slice(0, 3, &s2[..]);
    v2.set_bool_slice(2, 3, &s2x[..]);
    assert_eq!(v2.get(2).get_bool(), false);
    ddb::del_constant(v2.to_constant());
}

#[test]
fn Vector_set_short_slice() {
    let v3 = ddb::create_vector(ddb::DT_SHORT, 5);
    let s3: [c_short; 3] = [-1, -1, -1];
    v3.set_short_slice(1, 3, &s3[..]);
    assert_eq!(v3.get(3).get_short(), -1);
    ddb::del_constant(v3.to_constant());
}

#[test]
fn Vector_set_long_slice() {
    let v4 = ddb::create_vector(ddb::DT_LONG, 5);
    let s4: [c_long; 3] = [100000000000, 100000000000, 100000000000];
    v4.set_long_slice(0, 3, &s4[..]);
    assert_eq!(v4.get(0).get_long(), 100000000000);
    ddb::del_constant(v4.to_constant());
}

#[test]
fn Vector_set_float_slice() {
    let v5 = ddb::create_vector(ddb::DT_FLOAT, 10);
    let s5: [c_float; 3] = [1.7, 1.8, 1.9];
    v5.set_float_slice(0, 3, &s5[..]);
    assert_eq!(v5.get(2).get_float(), 1.9);
    ddb::del_constant(v5.to_constant());
}

#[test]
fn Vector_set_double_slice() {
    let v6 = ddb::create_vector(ddb::DT_DOUBLE, 3);
    let s6: [c_double; 3] = [1.11111, 2.33333, 3.33333];
    v6.set_double_slice(0, 3, &s6[..]);
    assert_eq!(v6.get(1).get_double(), 2.33333);
    ddb::del_constant(v6.to_constant());
}

#[test]
fn Vector_set_string_slice() {
    let v7 = ddb::create_vector(ddb::DT_STRING, 5);
    let s7: [&str; 3] = ["first", "second", "third"];
    v7.set_string_slice(0, 3, &s7[..]);
    assert_eq!(v7.get(0).get_string(), "first");
    ddb::del_constant(v7.to_constant());
}

#[test]
fn Vector_append_int() {
    let v1 = ddb::create_vector(ddb::DT_INT, 0);
    let s1: [c_int; 3] = [1, 1, 1];
    v1.append_int(&s1[..], 3);
    assert_eq!(v1.get(0).get_int(), 1);
    assert_eq!(v1.size(), 3);
    ddb::del_constant(v1.to_constant());
}

#[test]
fn Vector_append_bool() {
    let v2 = ddb::create_vector(ddb::DT_BOOL, 0);
    let s2: [bool; 3] = [true, true, true];
    let s2x: [bool; 3] = [false, false, false];
    v2.append_bool(&s2[..], 3);
    v2.append_bool(&s2x[..], 2);
    assert_eq!(v2.get(3).get_bool(), false);
    assert_eq!(v2.size(), 5);
    ddb::del_constant(v2.to_constant());
}

#[test]
fn Vector_append_short() {
    let v3 = ddb::create_vector(ddb::DT_SHORT, 0);
    let s3: [c_short; 3] = [-1, -1, -1];
    v3.append_short(&s3[..], 3);
    assert_eq!(v3.get(2).get_short(), -1);
    ddb::del_constant(v3.to_constant());
}

#[test]
fn Vector_append_long() {
    let v4 = ddb::create_vector(ddb::DT_LONG, 0);
    let s4: [c_long; 3] = [100000000000, 100000000000, 100000000000];
    v4.append_long(&s4[..], 2);
    assert_eq!(v4.get(0).get_long(), 100000000000);
    assert_eq!(v4.size(), 2);
    ddb::del_constant(v4.to_constant());
}

#[test]
fn Vector_append_float() {
    let v5 = ddb::create_vector(ddb::DT_FLOAT, 10);
    let s5: [c_float; 3] = [1.7, 1.8, 1.9];
    v5.append_float(&s5[..], 3);
    assert_eq!(v5.get(12).get_float(), 1.9);
    ddb::del_constant(v5.to_constant());
}

#[test]
fn Vector_append_double() {
    let v6 = ddb::create_vector(ddb::DT_DOUBLE, 3);
    let s6: [c_double; 3] = [1.11111, 2.33333, 3.33333];
    v6.append_double(&s6[..], 3);
    assert_eq!(v6.get(4).get_double(), 2.33333);
    ddb::del_constant(v6.to_constant());
}

#[test]
fn Vector_append_string() {
    let v7 = ddb::create_vector(ddb::DT_STRING, 0);
    let s7: [&str; 3] = ["first", "second", "third"];
    v7.append_string(&s7[..], 2);
    assert_eq!(v7.get(1).get_string(), "second");
    ddb::del_constant(v7.to_constant());
}

#[test]
fn Vector_to_vector() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    assert_eq!(v1.get(0).get_string(), "3");

    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_remove() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    v1.remove(1);
    assert_eq!(v1.size(), 4);
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_append() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    v1.remove(1);
    v1.append(&ddb::create_int(8));
    assert_eq!(v1.get(4).get_string(), "8");
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_set_get_name() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    v1.set_name("vector1");
    assert_eq!(v1.get_name(), "vector1");
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_set_get_capacity() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    assert_eq!(v1.get_capacity(), 5);
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_set_get_unit_length() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    assert_eq!(v1.get_unit_length(), 4);
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_set_get_unit_size() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    assert_eq!(v1.size(), 5);
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_get_sub_vector() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    let v2 = v1.get_sub_vector(0, 2);
    assert_eq!(v2.get_string(), "[3,6]");
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_fill() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    let v2 = v1.get_sub_vector(0, 2);
    v1.fill(1, 2, ddb::create_int(7));
    assert_eq!(v1.get(2).get_int(), 7);
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_reverse() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    let v2 = v1.get_sub_vector(0, 2);
    v1.reverse();
    assert_eq!(v1.get(0).get_int(), 9);
    conn.close();
    del_constant(v1.to_constant());
}

#[test]
fn Vector_replace() {
    let conn = ddb::DBConnection::new();
    conn.connect(TEST_HOST, TEST_PORT, TEST_USER, TEST_PASS);
    let c1 = conn.run("3 6 1 5 9");
    let v1 = c1.to_vector();
    let v2 = v1.get_sub_vector(0, 2);
    v1.replace(ddb::create_int(5), ddb::create_int(4));
    assert_eq!(v1.get(3).get_int(), 4);
    conn.close();
    del_constant(v1.to_constant());
}
