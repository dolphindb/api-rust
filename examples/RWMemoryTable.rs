extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_double, c_float, c_int, c_longlong, c_short};

use std::str;

static HOST: &str = "127.0.0.1";
static PORT: libc::c_int = 8848;
static USER: &str = "admin";
static PASS: &str = "123456";
const ROWNUM: usize = 10000;
fn main() {
    let conn = DBConnection::new();
    let bconn = conn.connect(HOST, PORT, USER, PASS);
    if bconn == false {
        println!("Fail to Connect!");
        return;
    }
    let script = "kt = keyedTable(`col_int, 2000:0, `col_int`col_short`col_long`col_float`col_double`col_bool`col_string,  [INT, SHORT, LONG, FLOAT, DOUBLE, BOOL, STRING]); ";
    conn.run(script);

    let coltypes: [c_int; 7] = [
        DT_INT, DT_SHORT, DT_LONG, DT_FLOAT, DT_DOUBLE, DT_BOOL, DT_STRING,
    ];
    let colnum = 7;

    let mut colv: Vec<ddb::Vector> = Vec::new();
    for i in 0..(colnum) {
        colv.push(create_vector(coltypes[i], 0));
    }
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

    let rows = ROWNUM as c_int;
    colv[0].append_int(&v0[..], rows);
    colv[1].append_short(&v1[..], rows);
    colv[2].append_long(&v2[..], rows);
    colv[3].append_float(&v3[..], rows);
    colv[4].append_double(&v4[..], rows);
    colv[5].append_bool(&v5[..], rows);
    colv[6].append_string(&v6[..], rows);
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
    let re2 = conn.run("select count(*) from kt");
    println!("{}", re2.get_string());

    let res = conn.run("select * from kt");
    let res_table = res.to_table();

    let mut res_col: Vec<ddb::Vector> = Vec::new();
    for i in 0..(colnum) {
        res_col.push(res_table.get_column(i as c_int));
    }

    let _v1 = res_col[0].get(0);

    println!("{} {}", res_table.rows(), " rows ");
    println!("{} {}", res_table.columns(), " columns ");

    let re1 = conn.run("select  top 5 * from kt");
    println!("{}", re1.get_string());
    let re3 = conn.run("select * from kt where col_int =30");

    let res_table = re3.to_table();
    for i in 0..(res_table.columns()) {
        print!("{}	", res_table.get_column_name(i));
    }
    println!("");
    let col0 = res_table.get_column_by_name("col_int");
    let col1 = res_table.get_column_by_name("col_short");
    let col2 = res_table.get_column_by_name("col_long");
    let col3 = res_table.get_column_by_name("col_float");
    let col4 = res_table.get_column(4);
    let col5 = res_table.get_column(5);
    let col6 = res_table.get_column(6);
    for i in 0..(res_table.rows()) {
        println!(
            "{}	{}		{}		{}		{}		{}		{}",
            col0.get(i).get_int(),
            col1.get(i).get_short(),
            col2.get(i).get_long(),
            col3.get(i).get_float(),
            col4.get(i).get_double(),
            col5.get(i).get_bool(),
            col6.get(i).get_string()
        );
    }
}
