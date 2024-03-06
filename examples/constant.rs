use ordered_float::OrderedFloat;
use rust_api::{
    client::ClientBuilder,
    types::{Basic, Constant, Scalar},
};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let script = String::from("a = 1.1;a;");
    let mut res = client.run_script(script).await.unwrap();

    let c = res.first_mut().unwrap();
    let d = c.as_scalar_mut().unwrap().as_double_mut().unwrap();

    d.set(Some(OrderedFloat::<f64>::from(2.2)));
    println!("{}, {:?}", d.data_type().to_u8(), d.get_double());
}
