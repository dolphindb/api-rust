use dolphindb::{client::ClientBuilder, types::*};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();
    let mut variables: HashMap<String, ConstantImpl> = HashMap::new();

    // basic examples
    variables.insert("Int".to_owned(), Int::new(1).into());
    variables.insert("Double".to_owned(), Double::new(1.0).into());
    variables.insert(
        "String".to_owned(),
        DolphinString::new("str".to_owned()).into(),
    );

    // numeric types
    variables.insert("myVoid".to_owned(), Void::new(()).into());
    variables.insert("myBool".to_owned(), Bool::new(true).into());
    variables.insert("myChar".to_owned(), Char::new('a' as i8).into());
    variables.insert("myShort".to_owned(), Short::new(1i16).into());
    variables.insert("myInt".to_owned(), Int::new(2i32).into());
    variables.insert("myLong".to_owned(), Long::new(3i64).into());
    variables.insert("myFloat".to_owned(), Float::new(1.1f32).into());
    variables.insert("myDouble".to_owned(), Double::new(1.2f64).into());
    variables.insert(
        "myDecimal32".to_owned(),
        Decimal32::from_raw(100i32, 1).unwrap().into(),
    );
    variables.insert(
        "myDecimal64".to_owned(),
        Decimal64::from_raw(10000i64, 2).unwrap().into(),
    );
    variables.insert(
        "myDecimal128".to_owned(),
        Decimal128::from_raw(1000000i128, 3).unwrap().into(),
    );

    // temporal types
    // unix timestamp (milliseconds)
    let unix_timestamp = 1735660800_000i64;
    variables.insert(
        "myDate".to_owned(),
        Date::from_raw(unix_timestamp / 86400_000).unwrap().into(),
    );
    variables.insert(
        "myDateTime".to_owned(),
        DateTime::from_raw((unix_timestamp / 1000) as i32)
            .unwrap()
            .into(),
    );
    variables.insert(
        "myTimestamp".to_owned(),
        Timestamp::from_raw(unix_timestamp).unwrap().into(),
    );
    variables.insert(
        "myNanoTimestamp".to_owned(),
        NanoTimestamp::from_raw(unix_timestamp * 1000_000)
            .unwrap()
            .into(),
    );
    variables.insert(
        "myMonth".to_owned(),
        Month::from_ym(2025, 1).unwrap().into(),
    );
    variables.insert(
        "myMinute".to_owned(),
        Minute::from_hm(17, 30).unwrap().into(),
    );
    variables.insert(
        "mySecond".to_owned(),
        Second::from_hms(17, 30, 0).unwrap().into(),
    );
    variables.insert(
        "myTime".to_owned(),
        Time::from_hms_milli(17, 30, 0, 100).unwrap().into(),
    );
    variables.insert(
        "myNanoTime".to_owned(),
        NanoTime::from_hms_nano(17, 30, 0, 100_000_000u32)
            .unwrap()
            .into(),
    );
    variables.insert(
        "myDateHour".to_owned(),
        DateHour::from_ymd_h(2025, 1, 1, 17).unwrap().into(),
    );

    // other types
    variables.insert(
        "myString".to_owned(),
        DolphinString::new("str".to_owned()).into(),
    );
    variables.insert("myBlob".to_owned(), Blob::new(vec![b'a']).into());
    variables.insert("mySymbol".to_owned(), Symbol::new("1".to_owned()).into());

    client.upload(&variables).await.unwrap();

    // Void is not uploaded
    // println!("myVoid: {}", client.run_script("myVoid").await.unwrap().unwrap());
    let vars = [
        "myBool",
        "myChar",
        "myShort",
        "myInt",
        "myLong",
        "myFloat",
        "myDouble",
        "myDecimal32",
        "myDecimal64",
        "myDecimal128",
        "myDate",
        "myTimestamp",
        "myNanoTimestamp",
        "myDateTime",
        "myMonth",
        "mySecond",
        "myTime",
        "myNanoTime",
        "myDateHour",
        "myString",
        "myBlob",
        "mySymbol",
    ];
    for var in vars {
        println!(
            "{}: {}",
            var,
            client.run_script(var).await.unwrap().unwrap()
        );
    }
}
