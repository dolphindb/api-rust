mod setup;
mod utils;

use core::f32;
use dolphindb::client::ClientBuilder;
use dolphindb::stream_client::{request::Request, subscriber::*};
use dolphindb::types::*;
use encoding::{all::GBK, EncoderTrap, Encoding};
use futures::StreamExt;
use regex::Regex;
use rstest::rstest;
use std::time::Duration;

use setup::settings::Config;

mod test_stream_subscribe_request {
    use super::*;

    // todo:RUS-58
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_subscribe_request_msg_as_table_true() {
        const STREAM_TABLE: &str = "test_stream_subscribe_request_msg_as_table_true";
        let conf = Config::new();
        let re =
            Regex::new(format!(r"{}:{}:\w*/{}/rust", conf.host, conf.port, STREAM_TABLE).as_str())
                .unwrap();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{STREAM_TABLE};
                        insert into {STREAM_TABLE} values(0i,1970.01.01d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        req.with_msg_as_table(true);
        req.with_offset(0);
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(1);
        let mut _index = 0;
        while let Some(msg) = subscriber.next().await {
            let expect = vector_build!(
                Any,
                Any::new(Int::new(_index).into()),
                Any::new(Date::from_raw(_index.into()).unwrap().into())
            );
            assert_eq!(*msg.msg(), expect.into());
            assert_eq!(msg.offset(), _index.into());
            assert!(re.is_match(msg.topic()));
            _index += 1;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_subscribe_request_offset_minus_one() {
        const STREAM_TABLE: &str = "test_stream_subscribe_request_offset_minus_one";
        let conf = Config::new();
        let re =
            Regex::new(format!(r"{}:{}:\w*/{}/rust", conf.host, conf.port, STREAM_TABLE).as_str())
                .unwrap();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{STREAM_TABLE};
                        insert into {STREAM_TABLE} values(0i..9i,1970.01.01d..1970.01.10d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        req.with_offset(-1);
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(10);
        let _ = client
            .run_script(
                format!("insert into {STREAM_TABLE} values(10i..19i,1970.01.11d..1970.01.20d)")
                    .as_str(),
            )
            .await
            .unwrap();
        let mut _index = 10;
        while let Some(msg) = subscriber.next().await {
            let expect = vector_build!(
                Any,
                Any::new(Int::new(_index).into()),
                Any::new(Date::from_raw(_index.into()).unwrap().into())
            );
            assert_eq!(*msg.msg(), expect.into());
            assert_eq!(msg.offset(), _index.into());
            assert!(re.is_match(msg.topic()));
            _index += 1;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[rstest]
    #[case::offset_zero(0)]
    #[case::offset_one(1)]
    #[case::offset_five(5)]
    async fn test_stream_subscribe_request_offset_natural_number(#[case] offset: i32) {
        let stream_table: String = format!(
            "test_stream_subscribe_request_offset_natural_number_{}",
            offset
        );
        let conf = Config::new();
        let re =
            Regex::new(format!(r"{}:{}:\w*/{}/rust", conf.host, conf.port, stream_table).as_str())
                .unwrap();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{stream_table};
                        insert into {stream_table} values(0i..9i,1970.01.01d..1970.01.10d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(stream_table.into(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        req.with_offset(offset.into());
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(10 - offset as usize);
        let mut _index = offset;
        while let Some(msg) = subscriber.next().await {
            let expect = vector_build!(
                Any,
                Any::new(Int::new(_index).into()),
                Any::new(Date::from_raw(_index.into()).unwrap().into())
            );
            assert_eq!(*msg.msg(), expect.into());
            assert_eq!(msg.offset(), _index.into());
            assert!(re.is_match(msg.topic()));
            _index += 1;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[rstest]
    #[case::reconnect_true(true, 2)]
    #[case::reconnect_false(false, 1)]
    async fn test_stream_subscribe_request_reconnect(#[case] reconnect: bool, #[case] expect: i32) {
        let stream_table: String = format!("test_stream_subscribe_request_reconnect_{}", reconnect);
        let conf = Config::new();
        let re =
            Regex::new(format!(r"{}:{}:\w*/{}/rust", conf.host, conf.port, stream_table).as_str())
                .unwrap();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{stream_table};
                        insert into {stream_table} values(0i,1970.01.01d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(stream_table.clone(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        req.with_reconnect(reconnect);
        req.with_reconnect_timeout(Duration::new(1, 0));
        req.with_offset(0);
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(2);
        let _ = client
        .run_script(
            format!(
                r#"
                    subscribers = select * from getStreamingStat().pubTables where tableName=`{stream_table};
                    for(subscriber in subscribers){{
                        ip_port = subscriber.subscriber.split(":");
                        stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
                    }}
                    insert into {stream_table} values(1i,1970.01.02d);
                "#
            )
            .as_str(),
        )
        .await
        .unwrap();
        let mut _index = 0;
        while let Some(msg) = subscriber.next().await {
            let _expect = vector_build!(
                Any,
                Any::new(Int::new(_index).into()),
                Any::new(Date::from_raw(_index.into()).unwrap().into())
            );
            assert_eq!(*msg.msg(), _expect.into());
            assert_eq!(msg.offset(), _index.into());
            assert!(re.is_match(msg.topic()));
            _index += 1;
        }
        assert_eq!(_index, expect);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_subscribe_request_filter_single() {
        const STREAM_TABLE: &str = "test_stream_subscribe_request_filter_single";
        let conf = Config::new();
        let re =
            Regex::new(format!(r"{}:{}:\w*/{}/rust", conf.host, conf.port, STREAM_TABLE).as_str())
                .unwrap();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{STREAM_TABLE};
                        {STREAM_TABLE}.setStreamTableFilterColumn(`data);
                        insert into {STREAM_TABLE} values(0i..9i,1970.01.01d..1970.01.10d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        let filter = vector_build!(Int, Int::new(1i32));
        req.with_filter(VectorImpl::Int(filter.clone()));
        req.with_offset(0);
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(1);
        let mut _index = 0;
        while let Some(msg) = subscriber.next().await {
            let data = *filter.get(_index).unwrap();
            let expect = vector_build!(
                Any,
                Any::new(data.into()),
                Any::new(
                    Date::from_raw(data.into_inner().unwrap().into())
                        .unwrap()
                        .into()
                )
            );
            assert_eq!(*msg.msg(), expect.into());
            assert!(re.is_match(msg.topic()));
            _index += 1;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_subscribe_request_filter_multi() {
        const STREAM_TABLE: &str = "test_stream_subscribe_request_filter_multi";
        let conf = Config::new();
        let re =
            Regex::new(format!(r"{}:{}:\w*/{}/rust", conf.host, conf.port, STREAM_TABLE).as_str())
                .unwrap();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{STREAM_TABLE};
                        {STREAM_TABLE}.setStreamTableFilterColumn(`data);
                        insert into {STREAM_TABLE} values(0i..9i,1970.01.01d..1970.01.10d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        let filter = vector_build!(Int, Int::new(1i32), Int::new(5i32));
        req.with_filter(VectorImpl::Int(filter.clone()));
        req.with_offset(0);
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(2);
        let mut _index = 0;
        while let Some(msg) = subscriber.next().await {
            let data = *filter.get(_index).unwrap();
            let expect = vector_build!(
                Any,
                Any::new(data.into()),
                Any::new(
                    Date::from_raw(data.into_inner().unwrap().into())
                        .unwrap()
                        .into()
                )
            );
            assert_eq!(*msg.msg(), expect.into());
            assert!(re.is_match(msg.topic()));
            _index += 1;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_subscribe_request_without_auth() {
        const STREAM_TABLE: &str = "test_stream_subscribe_request_without_auth";
        let conf = Config::new();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE]) as `{STREAM_TABLE};
                        {STREAM_TABLE}.setStreamTableFilterColumn(`data);
                        insert into {STREAM_TABLE} values(0i..9i,1970.01.01d..1970.01.10d);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_offset(0);
        assert!(builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_subscribe_request_table_not_exist() {
        const STREAM_TABLE: &str = "test_stream_subscribe_request_table_not_exist";
        let conf = Config::new();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        req.with_offset(0);
        assert!(builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .is_err());
    }
}

mod test_stream_subscribe_subscriber {
    use super::*;

    macro_rules! macro_test_stream_subscribe {
        ($name:ident, $($script:expr => $expect:expr),*) => {
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                const STREAM_TABLE: &str = stringify!($name);
                let conf = Config::new();
                let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
                c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
                let mut client = c_builder.connect().await.unwrap();
                let mut _index:u32 = 0;
                $(
                    _index += 1;
                    if _index == 1 {
                        let _ = client.run_script(
                            format!(
                                r#"   
                                    if (typestr({0})=="DECIMAL32"){{
                                        share streamTable(10:0,[`data],[DECIMAL32(2)]) as `{STREAM_TABLE}
                                    }} else if (typestr({0})=="DECIMAL64"){{
                                        share streamTable(10:0,[`data],[DECIMAL64(2)]) as `{STREAM_TABLE}
                                    }}else if (typestr({0})=="DECIMAL128"){{
                                        share streamTable(10:0,[`data],[DECIMAL128(2)]) as `{STREAM_TABLE}
                                    }} else {{
                                        share streamTable(10:0,[`data],[type({0})]) as `{STREAM_TABLE}
                                    }}
                                "#,
                                $script
                            )
                            .as_str(),
                        )
                        .await;
                    }
                    let _ = client.run_script(
                        format!(
                            "insert into {STREAM_TABLE} values({})",
                            $script
                        )
                        .as_str(),
                    )
                    .await;
                )*
                let mut builder = SubscriberBuilder::new();
                let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
                req.with_auth((conf.user, conf.passwd));
                req.with_offset(0);
                let mut subscriber = builder
                    .subscribe(format!("{}:{}", conf.host, conf.port), req)
                    .await
                    .unwrap()
                    .take(_index as usize);
                $(
                    let msg = subscriber.next().await.unwrap();
                    let expect = vector_build!(
                        Any,
                        Any::new($expect.into())
                    );
                    assert_eq!(*msg.msg(), expect.into());
                )*
            }
        };
    }

    // bool
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_bool,
        "true" => Bool::new(true),
        "false" => Bool::new(false),
        "00b" => Bool::default()
    );
    // char
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_char,
        "0c" => Char::new(0i8),
        "127c" => Char::new(127i8),
        "-127c" => Char::new(-127i8),
        "00c" => Char::default()
    );
    // short
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_short,
        "0h" => Short::new(0i16),
        "32767h" => Short::new(32767i16),
        "-32767h" => Short::new(-32767i16),
        "00h" => Short::default()
    );
    // int
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_int,
        "0i" => Int::new(0i32),
        "2147483647i" => Int::new(2147483647i32),
        "-2147483647i" => Int::new(-2147483647i32),
        "00i" => Int::default()
    );
    // long
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_long,
        "0l" => Long::new(0i64),
        "9223372036854775807l" => Long::new(9223372036854775807i64),
        "-9223372036854775807l" => Long::new(-9223372036854775807i64),
        "00l" => Long::default()
    );
    // date
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_date,
        "1970.01.01d" => Date::from_ymd(1970, 1, 1).unwrap(),
        "2022.05.20d" => Date::from_ymd(2022, 5, 20).unwrap(),
        "00d" => Date::default()
    );
    // month
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_month,
        "month(0)" => Month::from_ym(0, 1).unwrap(),
        "1970.01M" => Month::from_ym(1970, 1).unwrap(),
        "00M" => Month::default()
    );
    // time
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_time,
        "00:00:00.000t" => Time::from_hms_milli(0, 0, 0, 0).unwrap(),
        "13:50:59.123t" => Time::from_hms_milli(13, 50, 59, 123).unwrap(),
        "00t" => Time::default()
    );
    // minute
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_minute,
        "00:00m" => Minute::from_hm(0, 0).unwrap(),
        "13:50m" => Minute::from_hm(13, 50).unwrap(),
        "00m" => Minute::default()
    );
    // second
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_second,
        "00:00:00s" => Second::from_hms(0, 0, 0).unwrap(),
        "13:50:59s" => Second::from_hms(13, 50, 59).unwrap(),
        "00s" => Second::default()
    );
    // datetime
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_datetime,
        "1970.01.01T00:00:00D" => DateTime::from_raw(0i32).unwrap(),
        "2022.05.20T13:50:59D" =>
        DateTime::from_date_second(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Second::from_hms(13, 50, 59).unwrap()
        )
        .unwrap(),
        "00D" => DateTime::default()
    );
    // timestamp
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_timestamp,
        "1970.01.01T00:00:00.000T" => Timestamp::from_raw(0i64).unwrap(),
        "2022.05.20T13:50:59.123T" =>
        Timestamp::from_date_time(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap()
        )
        .unwrap(),
        "00T" => Timestamp::default()
    );
    // nanotime
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_nanotime,
        "00:00:00.000000000n" => NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
        "13:50:59.123456789n" => NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
        "00n" => NanoTime::default()
    );
    // nanotimestamp
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_nanotimestamp,
        "1970.01.01T00:00:00.000000000N" => NanoTimestamp::from_raw(0).unwrap(),
        "2022.05.20T13:50:59.123456789N" =>
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(2022, 5, 20).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
        )
        .unwrap(),
        "00N" => NanoTimestamp::default()
    );
    // datehour
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_datehour,
        "datehour('1970.01.01T00')" => DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
        "datehour('2022.05.20T13')" => DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
        "datehour(NULL)" => DateHour::default()
    );
    // float
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_float,
        "0.0f" => Float::new(0.0f32),
        "3.14f" => Float::new(3.14f32),
        "float('nan')" => Float::new(f32::NAN),
        "float('inf')" => Float::new(f32::INFINITY),
        "00f" => Float::default()
    );
    // double
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_double,
        "0.0F" => Double::new(0.0f64),
        "3.14F" => Double::new(3.14f64),
        "00F" => Double::default()
    );
    // string
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_string,
        "'abc!@#中文 123'" => DolphinString::new(String::from("abc!@#中文 123")),
        "\"\"" => DolphinString::default()
    );
    // blob
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_blob,
        "blob(\"abc!@#中文 123\")" => Blob::new("abc!@#中文 123".as_bytes().to_vec()),
        "blob(fromUTF8(\"abc!@#中文 123\",\"gbk\"))" => Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()),
        "blob(\"\")" => Blob::default()
    );
    // decimal32
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_decimal32,
        "decimal32(\"0\",3)" => Decimal32::from_raw(0i32, 2).unwrap(),
        "decimal32(\"3.141592653589\",8)" => Decimal32::from_raw(314i32, 2).unwrap(),
        "decimal32(\"-0.14159265358\",9)" => Decimal32::from_raw(-14i32, 2).unwrap(),
        "decimal32(\"nan\",9)" => Decimal32::default()
    );
    // decimal64
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_decimal64,
        "decimal64(\"0\",3)" => Decimal64::from_raw(0i64, 2).unwrap(),
        "decimal64(\"3.14159265358979323846\",17)" => Decimal64::from_raw(314i64, 2).unwrap(),
        "decimal64(\"-0.14159265358979323846\",18)" => Decimal64::from_raw(-14i64, 2).unwrap(),
        "decimal64(\"nan\",0)" => Decimal64::default()
    );
    // decimal128
    macro_test_stream_subscribe!(
        test_stream_subscribe_subscriber_decimal128,
        "decimal128(\"0\",3)" => Decimal128::from_raw(0i128, 2).unwrap(),
        "decimal128(\"3.14159265358979323846264338327950288419\",28)" => Decimal128::from_raw(314i128, 2).unwrap(),
        "decimal128(\"nan\",0)" => Decimal128::default()
    );

    #[tokio::test(flavor = "multi_thread")]
    async fn macro_test_stream_subscribe_array_vector() {
        const STREAM_TABLE: &str = "macro_test_stream_subscribe_array_vector";
        let conf = Config::new();
        let mut c_builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        c_builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = c_builder.connect().await.unwrap();
        let _ = client
            .run_script(
                format!(
                    "
                        share streamTable(10:0,`data`time,[INT,DATE[]]) as `{STREAM_TABLE};
                        insert into {STREAM_TABLE} values(0i,[1970.01.01d..1970.01.09d]);
                    "
                )
                .as_str(),
            )
            .await
            .unwrap();
        let mut builder = SubscriberBuilder::new();
        let mut req = Request::new(STREAM_TABLE.into(), "rust".into());
        req.with_auth((conf.user, conf.passwd));
        req.with_offset(0);
        let mut subscriber = builder
            .subscribe(format!("{}:{}", conf.host, conf.port), req)
            .await
            .unwrap()
            .take(1);
        assert!(subscriber.next().await.is_none());
    }
}
