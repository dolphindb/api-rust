extern crate ddb;
extern crate libc;
extern crate rand;

use crate::ddb::*;
use libc::c_int;

use std::str;

static HOST: &str = "127.0.0.1";
static PORT: c_int = 1621;

use rand::Rng;

fn main() {
    let mut rng = rand::thread_rng();
    let n1: c_int = rng.gen();
    let listenport: c_int = n1 % 1000 + 50000;
    let client = PollingClient::new(listenport);

    let queue = client.subscribe(HOST, PORT, "st1", &default_action_name(), 0);

    let msg = create_constant(DT_VOID);

    loop {
        if queue.poll(&msg, 1000) {
            if msg.is_null() {
                break;
            }

            println!("Get a message : {}", msg.get_string());
        }
    }
    client.unsubscribe(HOST, PORT, "st1", &default_action_name());
}
