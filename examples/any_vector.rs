use dolphindb::{
    client::ClientBuilder,
    types::{Any, ConstantImpl, Int, Vector, VectorImpl},
};

#[tokio::main]
async fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let mut v: Vector<Any> = Vector::new();
    v.push_raw(Int::new(1).into());

    let c: ConstantImpl = VectorImpl::Long(Vector::from_raw(&[1.into(), 2.into()])).into();
    v.push(c.into());
    let res = client
        .run_function("max", &vec![ConstantImpl::Vector(v.into())])
        .await
        .unwrap();

    if let Some(ref c) = res {
        println!("{}", c);
    }
}
