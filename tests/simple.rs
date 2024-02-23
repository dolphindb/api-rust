use rust_api::client::ClientBuilder;

#[tokio::test]
async fn simple_test() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    assert!(builder.connect().await.is_ok());
}
