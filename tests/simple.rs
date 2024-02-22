use rust_api::client::ClientBuilder;

#[tokio::test]
async fn auto_headers() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848").unwrap();
    builder.with_auth(("admin", "123456"));
    assert!(builder.connect().await.is_ok());
}
