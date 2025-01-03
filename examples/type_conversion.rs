use dolphindb::{
    client::ClientBuilder,
    types::*,
};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();
    let mut variables = HashMap::new();

    // basic examples
    variables.insert("Int".to_string(), ConstantImpl::from(Int::new(1)));
    variables.insert("Double".to_string(), ConstantImpl::from(Double::new(1.0)));
    variables.insert("String".to_string(), ConstantImpl::from(DolphinString::new(String::from("1"))));

    // numeric types
    variables.insert("myVoid".to_string(), ConstantImpl::from(Void::new(())));
    variables.insert("myBool".to_string(), ConstantImpl::from(Bool::new(true)));
    variables.insert("myChar".to_string(), ConstantImpl::from(Char::new('a' as i8)));
    variables.insert("myShort".to_string(), ConstantImpl::from(Short::new(1i16)));
    variables.insert("myInt".to_string(), ConstantImpl::from(Int::new(2i32)));
    variables.insert("myLong".to_string(), ConstantImpl::from(Long::new(3i64)));
    variables.insert("myFloat".to_string(), ConstantImpl::from(Float::new(1.1f32)));
    variables.insert("myDouble".to_string(), ConstantImpl::from(Double::new(1.2f64)));
    variables.insert("myDecimal32".to_string(), ConstantImpl::from(Decimal32::from_raw(100i32, 1).unwrap()));
    variables.insert("myDecimal64".to_string(), ConstantImpl::from(Decimal64::from_raw(10000i64, 2).unwrap()));
    variables.insert("myDecimal128".to_string(), ConstantImpl::from(Decimal128::from_raw(1000000i128, 3).unwrap()));

    // temporal types
    variables.insert("myDate".to_string(), ConstantImpl::from(Date::from_raw(20089).unwrap()));
    variables.insert("myTimestamp".to_string(), ConstantImpl::from(Timestamp::from_raw(1735660800_000i64).unwrap()));
    variables.insert("myNanoTimestamp".to_string(), ConstantImpl::from(NanoTimestamp::from_raw(1735660800_000_000_000i64).unwrap()));
    variables.insert("myMonth".to_string(), ConstantImpl::from(Month::from_ym(2025, 1).unwrap()));
    variables.insert("myMinute".to_string(), ConstantImpl::from(Minute::from_hm(17, 30).unwrap()));
    variables.insert("mySecond".to_string(), ConstantImpl::from(Second::from_hms(17, 30, 0).unwrap()));
    variables.insert("myTime".to_string(), ConstantImpl::from(Time::from_hms_milli(17, 30, 0, 100).unwrap()));
    variables.insert("myDateTime".to_string(), ConstantImpl::from(DateTime::from_raw(1735660800).unwrap()));
    variables.insert("myNanoTime".to_string(), ConstantImpl::from(NanoTime::from_hms_nano(17, 30,0,100_000_000u32).unwrap()));

    client.upload(&variables).await.unwrap();

    // Void is not uploaded
    // println!("myVoid: {}", client.run_script("myVoid").await.unwrap().unwrap());
    println!("myBool: {}", client.run_script("myBool").await.unwrap().unwrap());
    println!("myChar: {}", client.run_script("myChar").await.unwrap().unwrap());
    println!("myShort: {}", client.run_script("myShort").await.unwrap().unwrap());
    println!("myInt: {}", client.run_script("myInt").await.unwrap().unwrap());
    println!("myLong: {}", client.run_script("myLong").await.unwrap().unwrap());
    println!("myFloat: {}", client.run_script("myFloat").await.unwrap().unwrap());
    println!("myDouble: {}", client.run_script("myDouble").await.unwrap().unwrap());
    println!("myDecimal32: {}", client.run_script("myDecimal32").await.unwrap().unwrap());
    println!("myDecimal64: {}", client.run_script("myDecimal64").await.unwrap().unwrap());
    println!("myDecimal128: {}", client.run_script("myDecimal128").await.unwrap().unwrap());
    println!("myDate: {}", client.run_script("myDate").await.unwrap().unwrap());
    println!("myTimestamp: {}", client.run_script("myTimestamp").await.unwrap().unwrap());
    println!("myNanoTimestamp: {}", client.run_script("myNanoTimestamp").await.unwrap().unwrap());
    println!("myMonth: {}", client.run_script("myMonth").await.unwrap().unwrap());
    println!("mySecond: {}", client.run_script("mySecond").await.unwrap().unwrap());
    println!("myTime: {}", client.run_script("myTime").await.unwrap().unwrap());
    println!("myDateTime: {}", client.run_script("myDateTime").await.unwrap().unwrap());
    println!("myNanoTime: {}", client.run_script("myNanoTime").await.unwrap().unwrap());
}
