use rust_api::{client::ClientBuilder, types::Basic};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let script = String::from("a = \"aa\";a;");
    let res = client.run_script(script).await.unwrap();

    let c = res.first().unwrap();
    println!("{}", c.data_type().to_u8());
    // println!("{}", c.get_int().unwrap());
}
