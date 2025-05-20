mod setup;
mod utils;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use dolphindb::client::ClientBuilder;
use dolphindb::client::TableWriter;
use dolphindb::types::PrimitiveType;
use dolphindb::types::*;
use setup::settings::Config;

mod test_table_writer_type {
    use super::*;

    macro_rules! macro_test_table_writer_type {
        ($name:ident, $type_str:expr, ($($data:expr),*) => $expect:expr) => {
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                // connect
                const TABLE: &str = stringify!($name);
                let conf = Config::new();
                let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
                builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
                let mut client_writer = builder_writer.connect().await.unwrap();
                let mut builder_query = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
                builder_query.with_auth((conf.user.as_str(), conf.passwd.as_str()));
                let mut client_query = builder_query.connect().await.unwrap();
                // create table writer
                let _ = client_writer.run_script(
                    format!("share table(10:0,[`data],[{0}]) as `{TABLE}", $type_str).as_str()
                ).await;
                let mut table_writer = TableWriter::new(client_writer, TABLE, 1024).await;
                // append
                let mut _index: usize = 0;
                $(
                    let result = table_writer.append_row($data).await.unwrap();
                    assert!(result.is_none());
                    _index += 1;
                )*
                assert_eq!(table_writer.size(), _index);
                let result_flush = table_writer.flush().await.unwrap().unwrap();
                assert_eq!(result_flush, Int::new(_index as i32).into());
                assert_eq!(table_writer.size(), 0);
                let res_value = client_query
                    .run_script(format!(r#"
                        def compare_rust(source,target) {{
                            if (type(source)==ANY) {{
                                flag_=true
                                if (form(source)==DICT) {{
                                    for (key in source.keys()) {{
                                        res_=compare_rust(source[key],target[key])
                                        if (!res_) {{
                                            flag_=false
                                            break
                                        }}
                                    }}
                                    return flag_
                                }} else if (form(source)==VECTOR) {{
                                    for (i in source.size()){{
                                        res_=compare_rust(source[i],target[i])
                                        if (!res_) {{
                                            flag_=false
                                            break
                                        }}
                                    }}
                                    return flag_
                                }} else {{
                                    throw "not support form" 
                                }}
                            }} else {{
                                if (form(source)==DICT) {{
                                    return all(each(eqObj, sort(source.keys()), sort(target.keys())) <- each(eqObj, sort(source.values()), sort(target.values())))
                                }} else if (form(source)==TABLE) {{
                                    flag_=true
                                    for (key in source.keys()) {{
                                        res_=compare_rust(source[key],target[key])
                                        if (!res_) {{
                                            flag_=false
                                            break
                                        }}
                                    }}
                                    return flag_
                                }} else if (form(source)==VECTOR && type(source)>=VOID[]) {{
                                    return eqObj(source,target)
                                }} else {{
                                    return all(eq(source,target))
                                }}
                            }}
                        }}
                        compare_rust({TABLE},{0})
                    "#, $expect).as_str())
                    .await
                    .unwrap();
                assert_eq!(res_value.unwrap(), Bool::new(true).into());
            }
        };
    }

    // bool
    macro_test_table_writer_type!(
        test_table_writer_type_bool,
        "BOOL",
        (
            &mut vec![PrimitiveType::Bool(true)],
            &mut vec![PrimitiveType::Bool(false)],
            &mut vec![PrimitiveType::None]
        ) => "table([true,false,00b] as data)"
    );
    // char
    macro_test_table_writer_type!(
        test_table_writer_type_char,
        "CHAR",
        (
            &mut vec![PrimitiveType::I8(0i8)],
            &mut vec![PrimitiveType::I8(127i8)],
            &mut vec![PrimitiveType::I8(-127i8)],
            &mut vec![PrimitiveType::I8(-128i8)]
        ) => "table([0c,127c,-127c,00c] as data)"
    );
    // short
    macro_test_table_writer_type!(
        test_table_writer_type_short,
        "SHORT",
        (
            &mut vec![PrimitiveType::I16(0i16)],
            &mut vec![PrimitiveType::I16(32767i16)],
            &mut vec![PrimitiveType::I16(-32767i16)],
            &mut vec![PrimitiveType::I16(-32768i16)]
        ) => "table([0h,32767h,-32767h,00h] as data)"
    );
    // int
    macro_test_table_writer_type!(
        test_table_writer_type_int,
        "INT",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(2147483647i32)],
            &mut vec![PrimitiveType::I32(-2147483647i32)],
            &mut vec![PrimitiveType::I32(-2147483648i32)]
        ) => "table([0i,2147483647i,-2147483647i,00i] as data)"
    );
    // long
    macro_test_table_writer_type!(
        test_table_writer_type_long,
        "LONG",
        (
            &mut vec![PrimitiveType::I64(0i64)],
            &mut vec![PrimitiveType::I64(9223372036854775807i64)],
            &mut vec![PrimitiveType::I64(-9223372036854775807i64)],
            &mut vec![PrimitiveType::I64(-9223372036854775808i64)]
        ) => "table([0l,9223372036854775807l,-9223372036854775807l,00l] as data)"
    );
    // date
    macro_test_table_writer_type!(
        test_table_writer_type_date,
        "DATE",
        (
            &mut vec![PrimitiveType::NaiveDate(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())],
            &mut vec![PrimitiveType::NaiveDate(NaiveDate::from_ymd_opt(2025, 5, 13).unwrap())],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01d,2025.05.13d,00d] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_date_i32,
        "DATE",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(20221i32)],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01d,2025.05.13d,00d] as data)"
    );
    // month
    macro_test_table_writer_type!(
        test_table_writer_type_month,
        "MONTH",
        (
            &mut vec![PrimitiveType::NaiveDate(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())],
            &mut vec![PrimitiveType::NaiveDate(NaiveDate::from_ymd_opt(2025, 5, 13).unwrap())],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01M,2025.05M,00M] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_month_i32,
        "MONTH",
        (
            &mut vec![PrimitiveType::I32(23640i32)],
            &mut vec![PrimitiveType::I32(24304i32)],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01M,2025.05M,00M] as data)"
    );
    // time
    macro_test_table_writer_type!(
        test_table_writer_type_time,
        "TIME",
        (
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_milli_opt(0,0,0,0).unwrap())],
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_milli_opt(13,50,59,123).unwrap())],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00:00.000t,13:50:59.123t,00t] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_time_i32,
        "TIME",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(49859123i32)],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00:00.000t,13:50:59.123t,00t] as data)"
    );
    // minute
    macro_test_table_writer_type!(
        test_table_writer_type_minute,
        "MINUTE",
        (
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_milli_opt(0,0,0,0).unwrap())],
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_milli_opt(13,50,59,123).unwrap())],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00m,13:50m,00m] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_minute_i32,
        "MINUTE",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(830i32)],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00m,13:50m,00m] as data)"
    );
    // second
    macro_test_table_writer_type!(
        test_table_writer_type_second,
        "SECOND",
        (
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_opt(0, 0, 0).unwrap())],
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_opt(13, 50, 59).unwrap())],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00:00s,13:50:59s,00s] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_second_i32,
        "SECOND",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(49859i32)],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00:00s,13:50:59s,00s] as data)"
    );
    // datetime
    macro_test_table_writer_type!(
        test_table_writer_type_datetime,
        "DATETIME",
        (
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap()
            ))],
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2025, 5, 13).unwrap(),
                NaiveTime::from_hms_opt(13, 50, 59).unwrap()
            ))],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01T00:00:00D,2025.05.13T13:50:59D,00D] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_datetime_i32,
        "DATETIME",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(1747144259i32)],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01T00:00:00D,2025.05.13T13:50:59D,00D] as data)"
    );
    // timestamp
    macro_test_table_writer_type!(
        test_table_writer_type_timestamp,
        "TIMESTAMP",
        (
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap()
            ))],
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2025, 5, 13).unwrap(),
                NaiveTime::from_hms_milli_opt(13, 50, 59, 123).unwrap()
            ))],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01T00:00:00.000T,2025.05.13T13:50:59.123T,00T] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_timestamp_i64,
        "TIMESTAMP",
        (
            &mut vec![PrimitiveType::I64(0i64)],
            &mut vec![PrimitiveType::I64(1747144259123i64)],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01T00:00:00.000T,2025.05.13T13:50:59.123T,00T] as data)"
    );
    // nanotime
    macro_test_table_writer_type!(
        test_table_writer_type_nanotime,
        "NANOTIME",
        (
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap())],
            &mut vec![PrimitiveType::NaiveTime(NaiveTime::from_hms_nano_opt(13, 50, 59, 123456789).unwrap())],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00:00.000000000n,13:50:59.123456789n,00n] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_nanotime_i64,
        "NANOTIME",
        (
            &mut vec![PrimitiveType::I64(0i64)],
            &mut vec![PrimitiveType::I64(49859123456789i64)],
            &mut vec![PrimitiveType::None]
        ) => "table([00:00:00.000000000n,13:50:59.123456789n,00n] as data)"
    );
    // nanotimestamp
    macro_test_table_writer_type!(
        test_table_writer_type_nanotimestamp,
        "NANOTIMESTAMP",
        (
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap()
            ))],
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2025, 5, 13).unwrap(),
                NaiveTime::from_hms_nano_opt(13, 50, 59, 123456789).unwrap()
            ))],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01T00:00:00.000000000N,2025.05.13T13:50:59.123456789N,00N] as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_nanotimestamp_i64,
        "NANOTIMESTAMP",
        (
            &mut vec![PrimitiveType::I64(0i64)],
            &mut vec![PrimitiveType::I64(1747144259123456789i64)],
            &mut vec![PrimitiveType::None]
        ) => "table([1970.01.01T00:00:00.000000000N,2025.05.13T13:50:59.123456789N,00N] as data)"
    );
    // datehour
    macro_test_table_writer_type!(
        test_table_writer_type_datehour,
        "DATEHOUR",
        (
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap()
            ))],
            &mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2025, 5, 13).unwrap(),
                NaiveTime::from_hms_opt(13, 50, 59).unwrap()
            ))],
            &mut vec![PrimitiveType::None]
        ) => "table(datehour(['1970.01.01T00','2025.05.13T13',NULL]) as data)"
    );
    macro_test_table_writer_type!(
        test_table_writer_type_datehour_i32,
        "DATEHOUR",
        (
            &mut vec![PrimitiveType::I32(0i32)],
            &mut vec![PrimitiveType::I32(485317i32)],
            &mut vec![PrimitiveType::None]
        ) => "table(datehour(['1970.01.01T00','2025.05.13T13',NULL]) as data)"
    );
    // float
    macro_test_table_writer_type!(
        test_table_writer_type_float,
        "FLOAT",
        (
            &mut vec![PrimitiveType::F32(0.0f32)],
            &mut vec![PrimitiveType::F32(3.14f32)],
            &mut vec![PrimitiveType::None]
        ) => "table([0.0f,3.14f,00f] as data)"
    );
    // double
    macro_test_table_writer_type!(
        test_table_writer_type_double,
        "DOUBLE",
        (
            &mut vec![PrimitiveType::F64(0.0f64)],
            &mut vec![PrimitiveType::F64(3.14f64)],
            &mut vec![PrimitiveType::None]
        ) => "table([0.0F,3.14F,00F] as data)"
    );
    // symbol
    macro_test_table_writer_type!(
        test_table_writer_type_symbol,
        "SYMBOL",
        (
            &mut vec![PrimitiveType::String(String::from("abc!@#中文 123"))],
            &mut vec![PrimitiveType::None]
        ) => "table(symbol([\"abc!@#中文 123\",\"\"]) as data)"
    );
    // string
    macro_test_table_writer_type!(
        test_table_writer_type_string,
        "STRING",
        (
            &mut vec![PrimitiveType::String(String::from("abc!@#中文 123"))],
            &mut vec![PrimitiveType::None]
        ) => "table([\"abc!@#中文 123\",\"\"] as data)"
    );
    // blob
    // macro_test_table_writer_type!(
    //     test_table_writer_type_blob,
    //     "BLOB",
    //     (
    //         &mut vec![PrimitiveType::String(String::from("abc!@#中文 123"))],
    //         &mut vec![PrimitiveType::None]
    //     ) => "table(blob([\"abc!@#中文 123\",\"\"]) as data)"
    // );
    // char array vector
    macro_test_table_writer_type!(
        test_table_writer_type_char_array_vector,
        "CHAR[]",
        (
            &mut vec![PrimitiveType::VecI8(vec![0i8,127i8,-127i8,-128i8])],
            &mut vec![PrimitiveType::VecI8(vec![0i8,1i8,2i8])]
        ) => "table(array(CHAR[]).append!([[0c,127c,-127c,00c],[0c,1c,2c]]) as data)"
    );
    // short array vector
    macro_test_table_writer_type!(
        test_table_writer_type_short_array_vector,
        "SHORT[]",
        (
            &mut vec![PrimitiveType::VecI16(vec![0i16,32767i16,-32767i16,-32768i16])],
            &mut vec![PrimitiveType::VecI16(vec![0i16,1i16,2i16])]
        ) => "table(array(SHORT[]).append!([[0h,32767h,-32767h,00h],[0h,1h,2h]]) as data)"
    );
    // int array vector
    macro_test_table_writer_type!(
        test_table_writer_type_int_array_vector,
        "INT[]",
        (
            &mut vec![PrimitiveType::VecI32(vec![0i32,2147483647i32,-2147483647i32,-2147483648i32])],
            &mut vec![PrimitiveType::VecI32(vec![0i32,1i32,2i32])]
        ) => "table(array(INT[]).append!([[0i,2147483647i,-2147483647i,00i],[0i,1i,2i]]) as data)"
    );
    // long array vector
    macro_test_table_writer_type!(
        test_table_writer_type_long_array_vector,
        "LONG[]",
        (
            &mut vec![PrimitiveType::VecI64(vec![0i64,9223372036854775807i64,-9223372036854775807i64,-9223372036854775808i64])],
            &mut vec![PrimitiveType::VecI64(vec![0i64,1i64,2i64])]
        ) => "table(array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l],[0l,1l,2l]]) as data)"
    );
    // float array vector
    macro_test_table_writer_type!(
        test_table_writer_type_float_array_vector,
        "FLOAT[]",
        (
            &mut vec![PrimitiveType::VecF32(vec![0.0f32,3.14f32,f32::MIN])],
            &mut vec![PrimitiveType::VecF32(vec![3.14f32,3.15f32,3.16f32])]
        ) => "table(array(FLOAT[]).append!([[0.0f,3.14f,00f],[3.14f,3.15f,3.16f]]) as data)"
    );
    // double array vector
    macro_test_table_writer_type!(
        test_table_writer_type_double_array_vector,
        "DOUBLE[]",
        (
            &mut vec![PrimitiveType::VecF64(vec![0.0f64,3.14f64,f64::MIN])],
            &mut vec![PrimitiveType::VecF64(vec![3.14f64,3.15f64,3.16f64])]
        ) => "table(array(DOUBLE[]).append!([[0.0F,3.14F,00F],[3.14F,3.15F,3.16F]]) as data)"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "batch_size must be positive.")]
async fn test_table_writer_batch_size_0() {
    const TABLE: &str = "test_table_writer_batch_size_0";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer
        .run_script(format!("share table(10:0,[`data],[INT]) as `{TABLE}").as_str())
        .await;
    let _ = TableWriter::new(client_writer, TABLE, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "test_table_writer_columns_error has 1 columns, but 2 provided.")]
async fn test_table_writer_columns_error() {
    const TABLE: &str = "test_table_writer_columns_error";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer
        .run_script(format!("share table(10:0,[`data],[INT]) as `{TABLE}").as_str())
        .await;
    let mut table_writer = TableWriter::new(client_writer, TABLE, 1).await;
    let _ = table_writer
        .append_row(&mut vec![
            PrimitiveType::I32(0i32),
            PrimitiveType::I32(1i32),
        ])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "Failed to get schema for table test_table_writer_table_not_exist")]
async fn test_table_writer_table_not_exist() {
    const TABLE: &str = "test_table_writer_table_not_exist";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = TableWriter::new(client_writer, TABLE, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "not implemented")]
async fn test_table_writer_type_not_support() {
    const TABLE: &str = "test_table_writer_type_not_support";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer.run_script(
        format!("share table(10:0,`data1`data2`data3`data4,[DECIMAL32(2),DECIMAL64(3),DECIMAL128(4),ANY]) as `{TABLE}").as_str()
    ).await;
    let _ = TableWriter::new(client_writer, TABLE, 3).await;
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = "Failed to insert into column `data1`: type NaiveDate cannot be converted to Second."
)]
async fn test_table_writer_type_error_naive_date() {
    const TABLE: &str = "test_table_writer_type_error_naive_date";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer
        .run_script(format!("share table(10:0,[`data1],[SECOND]) as `{TABLE}").as_str())
        .await;
    let mut table_writer = TableWriter::new(client_writer, TABLE, 1).await;
    let _ = table_writer
        .append_row(&mut vec![PrimitiveType::NaiveDate(
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        )])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = "Failed to insert into column `data1`: type NaiveTime cannot be converted to Date."
)]
async fn test_table_writer_type_error_naive_time() {
    const TABLE: &str = "test_table_writer_type_error_naive_time";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer
        .run_script(format!("share table(10:0,[`data1],[DATE]) as `{TABLE}").as_str())
        .await;
    let mut table_writer = TableWriter::new(client_writer, TABLE, 1).await;
    let _ = table_writer
        .append_row(&mut vec![PrimitiveType::NaiveTime(
            NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
        )])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = "Failed to insert into column `data1`: type NaiveDateTime cannot be converted to Date."
)]
async fn test_table_writer_type_error_naive_date_time() {
    const TABLE: &str = "test_table_writer_type_error_naive_date_time";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer
        .run_script(format!("share table(10:0,[`data1],[DATE]) as `{TABLE}").as_str())
        .await;
    let mut table_writer = TableWriter::new(client_writer, TABLE, 1).await;
    let _ = table_writer
        .append_row(&mut vec![PrimitiveType::NaiveDateTime(NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ))])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_writer_auto_flush() {
    const TABLE: &str = "test_table_writer_auto_flush";
    // connect
    let conf = Config::new();
    let mut builder_writer = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder_writer.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client_writer = builder_writer.connect().await.unwrap();
    // create table writer
    let _ = client_writer
        .run_script(format!("share table(10:0,[`data],[INT]) as `{TABLE}").as_str())
        .await;
    let mut table_writer = TableWriter::new(client_writer, TABLE, 3).await;
    for i in 0..3 {
        let _ = table_writer
            .append_row(&mut vec![PrimitiveType::I32(i)])
            .await;
    }
    assert_eq!(table_writer.size(), 0);
}
