use std::collections::HashMap;

use rust_api::{
    client::ClientBuilder,
    types::{Bool, ConstantKind, DataType, DateHour, Dictionary, ScalarKind},
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
        ConstantKind::Scalar(ScalarKind::Bool(Bool::new(1))),
    );

    let mut dict = Dictionary::new(DataType::Bool);
    dict.insert(
        ScalarKind::DateHour(DateHour::from_datehour(1970, 1, 1, 1).unwrap()),
        ConstantKind::Scalar(ScalarKind::Bool(Bool::new(0))),
    );
    variables.insert("b".to_string(), ConstantKind::Dictionary(dict));
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
