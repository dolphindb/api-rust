use dolphindb::{
    client::ClientBuilder,
    error::Error,
    types::{ConstantImpl, DoubleArrayVector, Int, IntVector, TableBuilder, VectorImpl},
};

async fn table_writer() -> Result<(), Error> {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let mut prices = DoubleArrayVector::new();
    let price1 = vec![1.1, 2.2, 3.3];
    prices.push(price1);
    println!("{prices}");

    // write one row
    let c_int = ConstantImpl::from(Int::new(1));
    let c_double_array_vector = ConstantImpl::Vector(VectorImpl::from(prices.clone()));
    let res = client
        .run_function("tableInsert{testTable}", &[c_int, c_double_array_vector])
        .await
        .unwrap()
        .unwrap();
    println!("{res}");

    // write a table
    let price2 = vec![4.4, 5.5];
    prices.push(price2);
    let v_int = IntVector::from_raw(&[2, 3]).into();
    let v_double_array_vector = VectorImpl::from(prices);
    println!("{v_double_array_vector}");
    let mut builder = TableBuilder::new();
    builder.with_name("my_table".to_string());
    builder.with_contents(
        vec![v_int, v_double_array_vector],
        vec!["volume".to_string(), "price".to_string()],
    );
    let table = builder.build().unwrap();
    println!("{table}");
    let res = client
        .run_function("tableInsert{testTable}", &[table.into()])
        .await?
        .unwrap();
    println!("{res}");
    Ok(())
}

#[tokio::main]
async fn main() {
    /*
       DolphinDB script for creating testTable:
       ```
       share table(1:0, `a`b, "INT" "DOUBLE[]") as testTable
       ```
    */
    table_writer().await.unwrap();
}
