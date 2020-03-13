extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::c_int;

use std::str;

static HOST: &str = "127.0.0.1";
static PORT: libc::c_int = 8848;
static USER: &str = "admin";
static PASS: &str = "123456";
const ROWNUM: usize = 1000;
fn main() {
    let conn = DBConnection::new();
    let bconn = conn.connect(HOST, PORT, USER, PASS);
    if bconn == false {
        println!("Fail to Connect!");
        return;
    }
    let script = "t = table(100:0, `id`date`x , [INT, DATE, DOUBLE]); share t as tglobal;login(`admin, `123456); dbPath='dfs://datedb'; if(existsDatabase(dbPath))\ndropDatabase(dbPath); db=database(dbPath, VALUE, 2017.08.07..2017.08.11); tb=db.createPartitionedTable(t, `pt,`date)";
    conn.run(script);

    let v1 = create_vector(DT_INT, 0);
    let v2 = create_vector(DT_DATE, 0);
    let v3 = create_vector(DT_DOUBLE, 0);
    for i in 0..ROWNUM {
        v1.append(&create_int(i as c_int));
        v2.append(&create_date(2017, 8, 7 + (i as c_int) % 5));
        v3.append(&create_double(3.1415926));
    }
    let cols: [Vector; 3] = [v1, v2, v3];
    let colnames: [&str; 3] = ["id", "date", "x"];
    let t = create_table_by_vector(&colnames[..], &cols[..]);

    let args: [Constant; 1] = [t.to_constant()];
    conn.run_func("tableInsert{loadTable('dfs://datedb', `pt)}", &args[..]);

    let res = conn.run("select count(*) from loadTable('dfs://datedb', `pt)");
    println!("{}", res.get_string());
}
