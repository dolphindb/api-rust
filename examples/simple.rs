use std::collections::HashMap;

use rust_api::{
    client::ClientBuilder,
    types::{Bool, ConstantKind, DateHour, ScalarKind},
};

fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = rt.block_on(async move { builder.connect().await }).unwrap();
    println!("connect");

    let script = String::from("print \"aaaa\";print(\"b\")\na = 1..10;\na;");
    let res = rt
        .block_on(async { client.run_script(script).await })
        .unwrap();
    for c in res {
        println!("{:?}", c);
    }

    let mut variables = HashMap::new();
    variables.insert(
        "a".to_string(),
        ConstantKind::Scalar(ScalarKind::Bool(Bool::new(Some(true)))),
    );

    let mut map = HashMap::new();
    map.insert(
        ScalarKind::DateHour(DateHour::from_ymd_h(1970, 1, 1, 1)),
        ConstantKind::Scalar(ScalarKind::Bool(Bool::new(None))),
    );
    variables.insert("b".to_string(), ConstantKind::Dictionary(map));
    let res = rt
        .block_on(async { client.upload(variables).await })
        .unwrap();
    for c in res {
        println!("{:?}", c);
    }

    let script = String::from("b;");
    let res = rt
        .block_on(async { client.run_script(script).await })
        .unwrap();
    for c in res {
        println!("{:?}", c);
    }
}
