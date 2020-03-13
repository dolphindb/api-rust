extern crate ddb;
extern crate libc;
use crate::ddb::Defobj;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;

fn main() {
    let conn = ddb::DBConnection::new();
    conn.connect("localhost", 8848, "admin", "123456");
    let v = conn.run("`IBM`GOOG`YHOO");
    println!("{}", v.get_string());

    let v = ddb::create_vector(ddb::DT_STRING, 10);

    v.fill(2, 3, ddb::create_string("one"));
    println!("{}", v.get_string());
}
