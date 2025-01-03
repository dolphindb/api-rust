use std::collections::HashMap;

use dolphindb::{
    client::ClientBuilder,
    types::{Blob, Vector},
};

#[tokio::main]
async fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let mut variables = HashMap::new();

    let res = client
        .run_script("a = blob(`a);\nb = blob(`abc`de);")
        .await
        .unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    variables.insert("a".to_string(), Blob::new(vec![b'a']).into());

    let res = client.run_script("b").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    variables.insert(
        "b".to_string(),
        Vector::<Blob>::from_raw(&[&vec![b'a'], &vec![b'b']]).into(),
    );

    client.upload(&variables).await.unwrap();

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    let res = client.run_script("b").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }
}
