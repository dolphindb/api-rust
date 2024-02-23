use std::collections::HashMap;

use rust_api::{
    client::ClientBuilder,
    types::{Bool, ConstantImpl, DateHour, ScalarImpl, Vector, VectorImpl},
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
        ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(Some(true)))),
    );

    let mut map = HashMap::new();
    map.insert(
        ScalarImpl::DateHour(DateHour::from_ymd_h(1970, 1, 1, 1)),
        ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(None))),
    );
    variables.insert("b".to_string(), ConstantImpl::Dictionary(map));
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

    let v = VectorImpl::Decimal32(Vector::from_raw(&[
        1.into(),
        2.into(),
        3.into(),
        4.into(),
        5.into(),
        6.into(),
        7.into(),
        8.into(),
    ]));
    println!("{:?}", v);
    let func = String::from("max");
    let res = rt
        .block_on(async {
            client
                .run_function(func, vec![ConstantImpl::Vector(v)])
                .await
        })
        .unwrap();
    for c in res {
        println!("{:?}", c);
    }
}
