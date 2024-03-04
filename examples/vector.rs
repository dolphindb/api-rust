use rust_api::{client::ClientBuilder, types::VectorKind};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let script = String::from("a = [1.1, 2.2, 3.3];a;");
    let mut res = client.run_script(script).await.unwrap();
    let c = res.pop().unwrap();
    let v = VectorKind::try_from(c).unwrap();
    println!("{:?}", v)
}
