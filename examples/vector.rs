use rust_api::{client::ClientBuilder, types::Constant, types::Double, types::VectorKind};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("127.0.0.1:8848")
        .with_auth(("admin", "123456"))
        .connect()
        .await
        .unwrap();
    println!("connect successfully");

    let mut res = client
        .run_script(String::from("a = [1.1, 2.2, 3.3];a;"))
        .await
        .unwrap();
    let mut c = res.pop().unwrap();

    let v = c.as_vector_mut().unwrap();
    if let VectorKind::Double(vd) = v {
        vd.push(Double::new(1.1));
    }

    // let vd = Vector::<Double>::from(v);
    println!("{}", v)
}
