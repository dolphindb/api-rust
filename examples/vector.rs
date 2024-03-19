use std::collections::HashMap;

use rust_api::{
    client::ClientBuilder,
    types::{Constant, Double, VectorKind},
};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let mut res = client
        .run_script(String::from("a = [1.1, 2.2, 3.3];a;"))
        .await
        .unwrap();
    let mut c = res.pop().unwrap();

    let v = match c.as_vector_mut().unwrap() {
        VectorKind::Double(vd) => Some(vd),
        _ => None,
    }
    .unwrap();
    println!("{}", v);
    let raw = v.get_data_mut();
    raw.push(Double::from(4.4));
    raw.remove(1);
    raw.remove(1);
    println!("{}", v);

    let mut variables = HashMap::new();
    variables.insert(String::from("c"), c);
    client.upload(variables).await.unwrap();

    let res = client.run_script(String::from("c")).await.unwrap();
    for c in res {
        println!("{c}")
    }
}
