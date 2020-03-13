extern crate ddb;
extern crate libc;
use crate::ddb::*;
use libc::{c_int, c_longlong, c_uchar};
use std::str;

use std::time::Instant;

use std::panic;
use std::thread;

static HOSTS: [&str; 10] = [
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
    "127.0.0.1",
];
static PORTS: [c_int; 10] = [8848, 8848, 8848, 8848, 8848,8848, 8848, 8848, 8848, 8848];
static USER: &str = "admin";
static PASS: &str = "123456";

fn create_demo_table(
    rows: c_int,
    startp: c_uchar,
    pcount: c_uchar,
    starttime: c_int,
    time_inc: c_int,
) -> Table {
    let colnames: [&str; 11] = [
        "fwname",
        "filename",
        "source_address",
        "source_port",
        "destination_address",
        "destination_port",
        "nat_source_address",
        "nat_source_port",
        "starttime",
        "stoptime",
        "elapsed_time",
    ];
    let coltypes: [c_int; 11] = [
        DT_SYMBOL,
        DT_STRING,
        DT_IP,
        DT_INT,
        DT_IP,
        DT_INT,
        DT_IP,
        DT_INT,
        DT_DATETIME,
        DT_DATETIME,
        DT_INT,
    ];
    let colnum = 11;

    let table = create_table(&colnames[..], &coltypes[..], rows, rows);

    let mut colv: Vec<ddb::Vector> = Vec::new();
    for i in 0..(colnum) {
        colv.push(table.get_column(i));
    }
    let mut sip: [c_uchar; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let ip: [c_uchar; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    sip[3] = 192;
    sip[2] = startp;
    sip[1] = pcount;

    let spip = create_constant(DT_IP);

    for j in 1..255 {
        sip[0] = j as c_uchar;

        spip.set_binary(&sip[..]);
        let x = spip.get_hash(50) as c_uchar;
        if x >= startp && x < startp + pcount {
            break;
        }
    }

    for i in 0..rows {
        colv[0].set_string_by_index(i, "10.189.45.2:9000");
        colv[1].set_string_by_index(i, &startp.to_string());
        colv[2].set_binary_by_index(i, &sip[..]);

        colv[3].set_int_by_index(i, i as c_int);
        colv[4].set_binary_by_index(i, &ip[..]);
        colv[5].set_int_by_index(i, 2 * i as c_int);

        colv[6].set_by_index(i, parse_constant(DT_IP, "192.168.1.1"));
        colv[7].set_int_by_index(i, 3 * i as c_int);
        colv[8].set_long_by_index(i, (starttime + time_inc) as c_longlong);

        colv[9].set_long_by_index(i, (starttime + 100) as c_longlong);
        colv[10].set_int_by_index(i, i as c_int);
    }

    return table;
}

fn finsert(
    rows: c_int,
    startp: c_uchar,
    pcount: c_uchar,
    starttime: c_int,
    time_inc: c_int,
    p: c_int,
    inserttimes: c_int,
) {
    let stime = Instant::now();
    let conn = DBConnection::new();
    let success = conn.connect(&HOSTS[p as usize], PORTS[p as usize], USER, PASS);
    if !success {
        panic!("connect failed");
    }
    let t = create_demo_table(rows, startp, pcount, starttime, time_inc);
    let args: [Constant; 1] = [t.to_constant()];

    for _i in 0..inserttimes {
        conn.run_func(
            "tableInsert{loadTable('dfs://natlog', `natlogrecords)}",
            &args[..],
        );

    }
    println!(
        "Inserted {} rows  {} times used {} ms",
        rows,
        inserttimes,
        stime.elapsed().as_millis()
    );
}

fn main() {
    let thread_count = 10;

    let mut thread_handlers = Vec::new();
    let tablerows = 100000;
    let inserttimes = 100;

    let stime = Instant::now();
    for i in 0..thread_count {
	println!("thread {} started to create.",i);
        thread_handlers.push(thread::spawn(move || {
            finsert(
                tablerows,
                (i * 5 ) as c_uchar,
                (5) as c_uchar,
                (get_epoch_time() / 1000) as c_int,
                (i * 5) as c_int,
                i as c_int,
                inserttimes as c_int,
            );
        }))
    }

    for handler in thread_handlers {
        handler.join().map_err(|err| println!("{:?}", err)).ok();
    }
    
    let rows :u128 =tablerows as u128 * inserttimes * thread_count;

    println!(
        "Inserted {} rows, took a total of {} ms,{} records per second  ",
        rows,stime.elapsed().as_millis(),rows/stime.elapsed().as_millis() * 1000
    );

}