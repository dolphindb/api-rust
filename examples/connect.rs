use rust_api::{
    client::ClientBuilder,
    types::{ConstantImpl, Int, ScalarImpl},
};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let script = String::from("a = 1;a;");
    let res = client.run_script(script).await.unwrap();
    let value = match res.first() {
        Some(ConstantImpl::Scalar(ScalarImpl::Int(i))) => *i,
        _ => Int::new(None),
    };
    println!("a: {}", value.get().unwrap());
}
