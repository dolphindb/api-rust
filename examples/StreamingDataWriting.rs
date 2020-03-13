extern crate ddb;
extern crate libc;

use crate::ddb::*;
use libc::{c_double, c_float, c_int, c_longlong, c_short, c_uchar};
use std::panic;
use std::str;

static HOST: &str = "127.0.0.1";
static PORT: c_int = 1621;
static USER: &str = "admin";
static PASS: &str = "123456";

fn create_demo_table(
    rows: c_int,
    _startp: c_uchar,
    _pcount: c_uchar,
    _starttime: c_int,
    _time_inc: c_int,
) -> Table {
    let colnames: [&str; 22] = [
        "id",
        "cbool",
        "cchar",
        "cshort",
        "cint",
        "clong",
        "cdate",
        "cmonth",
        "ctime",
        "cminute",
        "csecond",
        "cdatetime",
        "ctimestamp",
        "cnanotime",
        "cnanotimestamp",
        "cfloat",
        "cdouble",
        "csymbol",
        "cstring",
        "cuuid",
        "cip",
        "cint128",
    ];

    let coltypes: [c_int; 22] = [
        DT_LONG,
        DT_BOOL,
        DT_CHAR,
        DT_SHORT,
        DT_INT,
        DT_LONG,
        DT_DATE,
        DT_MONTH,
        DT_TIME,
        DT_MINUTE,
        DT_SECOND,
        DT_DATETIME,
        DT_TIMESTAMP,
        DT_NANOTIME,
        DT_NANOTIMESTAMP,
        DT_FLOAT,
        DT_DOUBLE,
        DT_SYMBOL,
        DT_STRING,
        DT_UUID,
        DT_IP,
        DT_INT128,
    ];
    let colnum = 22;
    let table = create_table(&colnames[..], &coltypes[..], rows, rows);

    let mut colv: Vec<ddb::Vector> = Vec::new();
    for i in 0..(colnum) {
        colv.push(table.get_column(i));
    }

    let mut ip: [c_uchar; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    for i in 0..rows {
        colv[0].set_long_by_index(i, i as c_longlong);
        colv[1].set_bool_by_index(i, true);
        colv[2].set_bool_by_index(i, false);
        colv[3].set_short_by_index(i, i as c_short);
        colv[4].set_int_by_index(i, i as c_int);
        colv[5].set_long_by_index(i, i as c_longlong);
        colv[6].set_by_index(i, parse_constant(DT_DATE, "2020.01.01"));
        colv[7].set_int_by_index(i, 24240); // 2020.01M
        colv[8].set_int_by_index(i, i as c_int);
        colv[9].set_int_by_index(i, i as c_int);
        colv[10].set_int_by_index(i, i as c_int);
        colv[11].set_int_by_index(i, 1577836800 + i as c_int); // 2020.01.01 00:00:00+i
        colv[12].set_long_by_index(i, 1577836800000 + i as c_longlong); // 2020.01.01 00:00:00+i
        colv[13].set_long_by_index(i, i as c_longlong);
        colv[14].set_long_by_index(i, 1577836800000000000 + i as c_longlong); // 2020.01.01 00:00:00.000000000+i
        colv[15].set_float_by_index(i, i as c_float);
        colv[16].set_double_by_index(i, i as c_double);
        colv[17].set_string_by_index(i, "sym");
        colv[18].set_string_by_index(i, "abc");
        ip[15] = i as c_uchar;
        colv[19].set_binary_by_index(i, &ip[..]);
        colv[20].set_binary_by_index(i, &ip[..]);
        colv[21].set_binary_by_index(i, &ip[..]);
    }
    return table;
}
fn finsert(
    rows: c_int,
    startp: c_uchar,
    pcount: c_uchar,
    starttime: c_int,
    time_inc: c_int,
    _p: c_int,
    inserttimes: c_int,
) {
    let conn = DBConnection::new();
    let success = conn.connect(HOST, PORT, USER, PASS);
    if !success {
        panic!("connect failed");
    }
    let t = create_demo_table(rows, startp, pcount, starttime, time_inc);
    let args: [Constant; 1] = [t.to_constant()];

    for _i in 0..inserttimes {
        conn.run_func("tableInsert{objByName(`st1)}", &args[..]);
    }
}
fn main() {
    let tablerows = 100;
    let inserttimes = 10;
    let insert_count = 5;

    for i in 0..insert_count {
        finsert(
            tablerows,
            (i * 5 - 1) as c_uchar,
            (5) as c_uchar,
            (get_epoch_time() / 1000) as c_int,
            (i * 5) as c_int,
            i as c_int,
            inserttimes as c_int,
        );
    }

    let conn = DBConnection::new();
    conn.connect(HOST, PORT, USER, PASS);
    let res = conn.run("login(`admin, `123456); select count(*) from objByName(`st1)");
    println!("{}", res.get_string());
}
