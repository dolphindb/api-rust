use rust_api::client::ClientBuilder;
use rust_api::types::Constant;

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let mut res = client
        .run_script(String::from(
            "aaa = table(`1 `2 `3 `4 as id, 10 20 30 40  as v1, 1.1 2.2 3.3 4.4 as v2);aaa;",
        ))
        .await
        .unwrap();
    let c = res.pop().unwrap();

    let tb = c.as_vector().unwrap();
    println!("{}", tb);
}
