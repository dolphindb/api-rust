use dolphindb::client::ClientBuilder;

#[tokio::main]
async fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let res = client.run_script("a = symbol(`a`b`c)").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }
}
