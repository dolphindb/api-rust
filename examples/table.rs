use std::collections::HashMap;

use dolphindb::{
    client::ClientBuilder,
    types::{DolphinString, Double, Int, TableBuilder, Vector},
};

#[tokio::main]
async fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let res = client.run_script("a = table(1..10 as id);").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    let mut builder = TableBuilder::new();
    builder.with_name("abc".to_string());
    builder.with_contents(
        vec![
            Vector::<Int>::from_raw(&[1, 2, 3]).into(),
            Vector::<Double>::from_raw(&[1.into(), 2.into(), 3.into()]).into(),
            Vector::<DolphinString>::from_raw(&["d", "e", "f"]).into(),
        ],
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
    );

    let table = builder.build().unwrap();

    let mut variables = HashMap::new();
    variables.insert("a".to_string(), table.into());

    client.upload(&variables).await.unwrap();

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }
}
