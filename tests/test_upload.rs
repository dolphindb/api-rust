mod setup;
mod utils;

use encoding::{all::GBK, EncoderTrap, Encoding};
use std::collections::HashMap;

use dolphindb::client::ClientBuilder;
use dolphindb::types::*;
use setup::settings::Config;

macro_rules! macro_test_upload {
    ($name:ident, $($var:expr => $expect:expr),*) => {
        #[tokio::test]
        async fn $name() {
            // connect
            let conf = Config::new();
            let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
            builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
            let mut client = builder.connect().await.unwrap();
            // upload
            let mut _upload = HashMap::<String, ConstantImpl>::new();
            let mut index_var:i8 = -1;
            $(
                index_var += 1;
                _upload.insert(format!("test_upload_{}",index_var), $var.into());
            )*
            let _ = client.upload(&_upload).await;
            // assert
            let mut index_expect:i8 = -1;
            $(
                index_expect += 1;
                // todo:
                let res_ = client.run_script(format!("test_upload_{}", index_expect).as_str()).await.unwrap().unwrap();
                println!("{}",res_);
                let res_form = client
                    .run_script(format!("form(test_upload_{})==form({})", index_expect, $expect).as_str())
                    .await
                    .unwrap();
                assert_eq!(res_form.unwrap(), Bool::new(true).into(),"test_upload_{} form assert error", index_expect);
                println!("{:?}",client
                .run_script(format!("typestr(test_upload_{})", index_expect).as_str())
                .await
                .unwrap());
                let res_type = client
                    .run_script(format!("typestr(test_upload_{})==typestr({})", index_expect, $expect).as_str())
                    .await
                    .unwrap();
                assert_eq!(res_type.unwrap(), Bool::new(true).into(),"test_upload_{} type assert error", index_expect);
                let res_value = client
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
                                }} else {{
                                    return all(eq(source,target))
                                }}
                            }}
                        }}
                        compare_rust(test_upload_{0},{1})
                    "#, index_expect, $expect).as_str())
                    .await
                    .unwrap();
                assert_eq!(res_value.unwrap(), Bool::new(true).into(),"test_upload_{} value assert error", index_expect);
            )*
        }
    };
}

mod test_upload_scalar {
    use super::*;

    // void
    // todo:bug?
    // macro_test_upload!(
    //     test_upload_scalar_void,
    //     Void::default() => "NULL"
    // );
    // bool
    macro_test_upload!(
        test_upload_scalar_bool,
        Bool::new(true) => "true",
        Bool::new(false) => "false",
        Bool::default() => "00b"
    );
    // char
    macro_test_upload!(
        test_upload_scalar_char,
        Char::new(0i8) => "0c",
        Char::new(127i8) => "127c",
        Char::new(-127i8) => "-127c",
        Char::default() => "00c"
    );
    // short
    macro_test_upload!(
        test_upload_scalar_short,
        Short::new(0i16) => "0h",
        Short::new(32767i16) => "32767h",
        Short::new(-32767i16) => "-32767h",
        Short::default() => "00h"
    );
    // int
    macro_test_upload!(
        test_upload_scalar_int,
        Int::new(0i32) => "0i",
        Int::new(2147483647i32) => "2147483647i",
        Int::new(-2147483647i32) => "-2147483647i",
        Int::default() => "00i"
    );
    // long
    macro_test_upload!(
        test_upload_scalar_long,
        Long::new(0i64) => "0l",
        Long::new(9223372036854775807i64) => "9223372036854775807l",
        Long::new(-9223372036854775807i64) => "-9223372036854775807l",
        Long::default() => "00l"
    );
    // date
    macro_test_upload!(
        test_upload_scalar_date,
        Date::from_ymd(1970, 1, 1).unwrap() => "1970.01.01d",
        Date::from_ymd(2022, 5, 20).unwrap() => "2022.05.20d",
        Date::default() => "00d"
    );
    // month
    macro_test_upload!(
        test_upload_scalar_month,
        Month::from_ym(1970, 1).unwrap() => "1970.01M",
        Month::from_ym(2022, 5).unwrap() => "2022.05M",
        Month::default() => "00M"
    );
    // time
    macro_test_upload!(
        test_upload_scalar_time,
        Time::from_hms_milli(0, 0, 0, 0).unwrap() => "00:00:00.000t",
        Time::from_hms_milli(13, 50, 59, 123).unwrap() => "13:50:59.123t",
        Time::default() => "00t"
    );
    // minute
    macro_test_upload!(
        test_upload_scalar_minute,
        Minute::from_hm(0, 0).unwrap() => "00:00m",
        Minute::from_hm(13, 50).unwrap() => "13:50m",
        Minute::default() => "00m"
    );
    // second
    macro_test_upload!(
        test_upload_scalar_second,
        Second::from_hms(0, 0, 0).unwrap() => "00:00:00s",
        Second::from_hms(13, 50, 59).unwrap() => "13:50:59s",
        Second::default() => "00s"
    );
    // datetime
    macro_test_upload!(
        test_upload_scalar_datetime,
        DateTime::from_date_second(
            Date::from_ymd(1970, 1, 1).unwrap(),
            Second::from_hms(0, 0, 0).unwrap()
        ).unwrap() => "1970.01.01T00:00:00D",
        DateTime::from_date_second(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Second::from_hms(13, 50, 59).unwrap()
        ).unwrap() => "2022.05.20T13:50:59D",
        DateTime::default() => "00D"
    );
    // timestamp
    macro_test_upload!(
        test_upload_scalar_timestamp,
        Timestamp::from_date_time(
            Date::from_ymd(1970, 1, 1).unwrap(),
            Time::from_hms_milli(0, 0, 0, 0).unwrap()
        ).unwrap() => "1970.01.01T00:00:00.000T",
        Timestamp::from_date_time(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap()
        ).unwrap() => "2022.05.20T13:50:59.123T",
        Timestamp::default() => "00T"
    );
    // nanotime
    macro_test_upload!(
        test_upload_scalar_nanotime,
        NanoTime::from_hms_nano(0, 0, 0, 0).unwrap() => "00:00:00.000000000n",
        NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap() => "13:50:59.123456789n",
        NanoTime::default() => "00n"
    );
    // nanotimestamp
    macro_test_upload!(
        test_upload_scalar_nanotimestamp,
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(1970, 1, 1).unwrap(),
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
        ).unwrap() => "1970.01.01T00:00:00.000000000N",
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(2022, 5, 20).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
        ).unwrap() => "2022.05.20T13:50:59.123456789N",
        NanoTimestamp::default() => "00N"
    );
    // datehour
    macro_test_upload!(
        test_upload_scalar_datehour,
        DateHour::from_ymd_h(1970, 1, 1, 0).unwrap() => "datehour(\"1970.01.01T00\")",
        DateHour::from_ymd_h(2022, 5, 20, 13).unwrap() => "datehour(\"2022.05.20T13\")",
        DateHour::default() => "datehour(NULL)"
    );
    // float
    macro_test_upload!(
        test_upload_scalar_float,
        Float::new(0.0f32) => "0.0f",
        Float::new(3.14f32) => "3.14f",
        // Float::new(f32::NAN) => "float(\"nan\")", todo: nan!=nan
        // Float::new(f32::INFINITY) => "float(\"inf\")", todo: inf!=inf
        Float::default() => "00f"
    );
    // double
    macro_test_upload!(
        test_upload_scalar_double,
        Double::new(0.0f64) => "0.0F",
        Double::new(3.14f64) => "3.14F",
        Double::default() => "00F"
    );
    // symbol
    macro_test_upload!(
        test_upload_scalar_symbol,
        Symbol::new(String::from("abc!@#中文 123")) => "\"abc!@#中文 123\"",
        Symbol::default() => "\"\""
    );
    // string
    macro_test_upload!(
        test_upload_scalar_string,
        DolphinString::new(String::from("abc!@#中文 123")) => "\"abc!@#中文 123\"",
        DolphinString::default() => "\"\""
    );
    // blob
    macro_test_upload!(
        test_upload_scalar_blob,
        Blob::new("abc!@#中文 123".as_bytes().to_vec()) => "blob(\"abc!@#中文 123\")",
        Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()) => "blob(fromUTF8(\"abc!@#中文 123\",\"gbk\"))",
        Blob::default() => "blob(\"\")"
    );
    // decimal32
    macro_test_upload!(
        test_upload_scalar_decimal32,
        Decimal32::from_raw(0i32, 3).unwrap() => "decimal32(\"0\",3)",
        Decimal32::from_raw(314159265i32, 8).unwrap() => "decimal32(\"3.141592653589\",8)",
        Decimal32::from_raw(-141592654i32, 9).unwrap() => "decimal32(\"-0.14159265358\",9)",
        Decimal32::default() => "decimal32(NULL,0)"
    );
    // decimal64
    macro_test_upload!(
        test_upload_scalar_decimal64,
        Decimal64::from_raw(0i64, 3).unwrap() => "decimal64(\"0\",3)",
        Decimal64::from_raw(314159265358979324i64, 17).unwrap() => "decimal64(\"3.14159265358979323846\",17)",
        Decimal64::from_raw(-141592653589793238i64, 18).unwrap() => "decimal64(\"-0.14159265358979323846\",18)",
        Decimal64::default() => "decimal64(NULL,0)"
    );
    // decimal128
    macro_test_upload!(
        test_upload_scalar_decimal128,
        Decimal128::from_raw(0i128, 3).unwrap() => "decimal128(\"0\",3)",
        Decimal128::from_raw(31415926535897932384626433833i128, 28).unwrap() => "decimal128(\"3.14159265358979323846264338327950288419\",28)",
        // Decimal128::from_raw(31415926535897932384626433832795028842i128, 37).unwrap() => "decimal128(\"3.14159265358979323846264338327950288419\",37)",
        // Decimal128::from_raw(-14159265358979323846264338327950288419i128, 38).unwrap() => "decimal128(\"-0.14159265358979323846264338327950288419\",38)",
        Decimal128::default() => "decimal128(NULL,0)"
    );
}

mod test_upload_pair {
    use super::*;

    // bool
    macro_test_upload!(
        test_upload_pair_bool,
        PairImpl::Bool(Pair::new((
            Bool::new(true),
            Bool::new(false)
        ))) => "pair(true,false)",
        PairImpl::Bool(Pair::new((
            Bool::new(true),
            Bool::default()
        ))) => "pair(true,00b)"
    );
    // char
    macro_test_upload!(
        test_upload_pair_char,
        PairImpl::Char(Pair::new((
            Char::new(0i8),
            Char::new(127i8)
        ))) => "pair(0c,127c)",
        PairImpl::Char(Pair::new((
            Char::new(-127i8),
            Char::default()
        ))) => "pair(-127c,00c)"
    );
    // short
    macro_test_upload!(
        test_upload_pair_short,
        PairImpl::Short(Pair::new((
            Short::new(0i16),
            Short::new(32767i16)
        ))) => "pair(0h,32767h)",
        PairImpl::Short(Pair::new((
            Short::new(-32767i16),
            Short::default()
        ))) => "pair(-32767h,00h)"
    );
    // int
    macro_test_upload!(
        test_upload_pair_int,
        PairImpl::Int(Pair::new((
            Int::new(0i32),
            Int::new(2147483647i32)
        ))) => "pair(0i,2147483647i)",
        PairImpl::Int(Pair::new((
            Int::new(-2147483647i32),
            Int::default()
        ))) => "pair(-2147483647i,00i)"
    );
    // long
    macro_test_upload!(
        test_upload_pair_long,
        PairImpl::Long(Pair::new((
            Long::new(0i64),
            Long::new(9223372036854775807i64)
        ))) => "pair(0l,9223372036854775807l)",
        PairImpl::Long(Pair::new((
            Long::new(-9223372036854775807i64),
            Long::default()
        ))) => "pair(-9223372036854775807l,00l)"
    );
    // date
    macro_test_upload!(
        test_upload_pair_date,
        PairImpl::Date(Pair::new((
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(1970, 1, 2).unwrap()
        ))) => "pair(1970.01.01d,1970.01.02d)",
        PairImpl::Date(Pair::new((
            Date::from_ymd(2022, 5, 20).unwrap(),
            Date::default()
        ))) => "pair(2022.05.20d,00d)"
    );
    // month
    macro_test_upload!(
        test_upload_pair_month,
        PairImpl::Month(Pair::new((
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(1970, 2).unwrap()
        ))) => "pair(1970.01M,1970.02M)",
        PairImpl::Month(Pair::new((
            Month::from_ym(2022, 5).unwrap(),
            Month::default()
        ))) => "pair(2022.05M,00M)"
    );
    // time
    macro_test_upload!(
        test_upload_pair_time,
        PairImpl::Time(Pair::new((
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(0, 0, 0, 1).unwrap()
        ))) => "pair(00:00:00.000t,00:00:00.001t)",
        PairImpl::Time(Pair::new((
            Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            Time::default()
        ))) => "pair(13:50:59.123t,00t)"
    );
    // minute
    macro_test_upload!(
        test_upload_pair_minute,
        PairImpl::Minute(Pair::new((
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(0, 1).unwrap()
        ))) => "pair(00:00m,00:01m)",
        PairImpl::Minute(Pair::new((
            Minute::from_hm(13, 50).unwrap(),
            Minute::default()
        ))) => "pair(13:50m,00m)"
    );
    // second
    macro_test_upload!(
        test_upload_pair_second,
        PairImpl::Second(Pair::new((
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(0, 0, 1).unwrap()
        ))) => "pair(00:00:00s,00:00:01s)",
        PairImpl::Second(Pair::new((
            Second::from_hms(13, 50, 59).unwrap(),
            Second::default()
        ))) => "pair(13:50:59s,00s)"
    );
    // datetime
    macro_test_upload!(
        test_upload_pair_datetime,
        PairImpl::DateTime(Pair::new((
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            ).unwrap(),
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 1).unwrap()
            ).unwrap(),
        ))) => "pair(1970.01.01T00:00:00D,1970.01.01T00:00:01D)",
        PairImpl::DateTime(Pair::new((
            DateTime::from_date_second(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Second::from_hms(13, 50, 59).unwrap()
            ).unwrap(),
            DateTime::default()
        ))) => "pair(2022.05.20T13:50:59D,00D)"
    );
    // timestamp
    macro_test_upload!(
        test_upload_pair_timestamp,
        PairImpl::Timestamp(Pair::new((
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            ).unwrap(),
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 1).unwrap()
            ).unwrap(),
        ))) => "pair(1970.01.01T00:00:00.000T,1970.01.01T00:00:00.001T)",
        PairImpl::Timestamp(Pair::new((
            Timestamp::from_date_time(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap()
            ).unwrap(),
            Timestamp::default()
        ))) => "pair(2022.05.20T13:50:59.123T,00T)"
    );
    // nanotime
    macro_test_upload!(
        test_upload_pair_nanotime,
        PairImpl::NanoTime(Pair::new((
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(0, 0, 0, 1).unwrap(),
        ))) => "pair(00:00:00.000000000n,00:00:00.000000001n)",
        PairImpl::NanoTime(Pair::new((
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            NanoTime::default()
        ))) => "pair(13:50:59.123456789n,00n)"
    );
    // nanotimestamp
    macro_test_upload!(
        test_upload_pair_nanotimestamp,
        PairImpl::NanoTimestamp(Pair::new((
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            ).unwrap(),
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 1).unwrap()
            ).unwrap(),
        ))) => "pair(1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000001N)",
        PairImpl::NanoTimestamp(Pair::new((
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            ).unwrap(),
            NanoTimestamp::default()
        ))) => "pair(2022.05.20T13:50:59.123456789N,00N)"
    );
    // datehour
    macro_test_upload!(
        test_upload_pair_datehour,
        PairImpl::DateHour(Pair::new((
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(1970, 1, 1, 1).unwrap(),
        ))) => "pair(datehour('1970.01.01T00'),datehour('1970.01.01T01'))",
        PairImpl::DateHour(Pair::new((
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DateHour::default()
        ))) => "pair(datehour('2022.05.20T13'),datehour(NULL))"
    );
    // float
    // todo:inf nan
    macro_test_upload!(
        test_upload_pair_float,
        PairImpl::Float(Pair::new((
            Float::new(0.0f32),
            Float::new(3.14f32),
        ))) => "pair(0.0f,3.14f)",
        PairImpl::Float(Pair::new((
            Float::new(3.14f32),
            Float::default()
        ))) => "pair(3.14f,00f)"
    );
    // double
    macro_test_upload!(
        test_upload_pair_double,
        PairImpl::Double(Pair::new((
            Double::new(0.0f64),
            Double::new(3.14f64),
        ))) => "pair(0.0F,3.14F)",
        PairImpl::Double(Pair::new((
            Double::new(3.14f64),
            Double::default()
        ))) => "pair(3.14F,00F)"
    );
    // symbol
    macro_test_upload!(
        test_upload_pair_symbol,
        PairImpl::Symbol(Pair::new((
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::default(),
        ))) => "symbol(pair(\"abc!@#中文 123\",\"\"))"
    );
    // string
    macro_test_upload!(
        test_upload_pair_string,
        PairImpl::String(Pair::new((
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::default(),
        ))) => "pair(\"abc!@#中文 123\",\"\")"
    );
    // blob
    // todo:
    // macro_test_upload!(
    //     test_upload_pair_blob,
    //     PairImpl::Blob(Pair::new((
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()),
    //     ))) => "pair(blob(\"abc!@#中文 123\"),blob(fromUTF8(\"abc!@#中文 123\",\"gbk\"))",
    //     PairImpl::Blob(Pair::new((
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()),
    //     ))) => "pair(blob(\"abc!@#中文 123\"),blob(\"\"))"
    // );
    // decimal32
    macro_test_upload!(
        test_upload_pair_decimal32,
        PairImpl::Decimal32(Pair::new((
            Decimal32::from_raw(314i32, 2).unwrap(),
            Decimal32::from_raw(315i32, 2).unwrap(),
        ))) => "pair(decimal32('3.14',2),decimal32('3.15',2))",
        PairImpl::Decimal32(Pair::new((
            Decimal32::from_raw(314i32, 2).unwrap(),
            Decimal32::default()
        ))) => "pair(decimal32('3.14',2),decimal32(NULL,2))"
    );
    // decimal64
    macro_test_upload!(
        test_upload_pair_decimal64,
        PairImpl::Decimal64(Pair::new((
            Decimal64::from_raw(314i64, 2).unwrap(),
            Decimal64::from_raw(315i64, 2).unwrap(),
        ))) => "pair(decimal64('3.14',2),decimal64('3.15',2))",
        PairImpl::Decimal64(Pair::new((
            Decimal64::from_raw(314i64, 2).unwrap(),
            Decimal64::default()
        ))) => "pair(decimal64('3.14',2),decimal64(NULL,2))"
    );
    // decimal128
    macro_test_upload!(
        test_upload_pair_decimal128,
        PairImpl::Decimal128(Pair::new((
            Decimal128::from_raw(314i128, 2).unwrap(),
            Decimal128::from_raw(315i128, 2).unwrap(),
        ))) => "pair(decimal128('3.14',2),decimal128('3.15',2))",
        PairImpl::Decimal128(Pair::new((
            Decimal128::from_raw(314i128, 2).unwrap(),
            Decimal128::default()
        ))) => "pair(decimal128('3.14',2),decimal128(NULL,2))"
    );
}

mod test_upload_vector {
    use super::*;

    macro_test_upload!(
        test_upload_vector_bool,
        VectorImpl::Bool(vector_build!(
            Bool,
            Bool::new(true),
            Bool::new(false),
            Bool::default()
        )) => "[true,false,00b]",
        VectorImpl::Bool(
            Vector::<Bool>::new()
        ) => "array(BOOL)",
        VectorImpl::Bool(vector_build!(
            Bool,
            Bool::default(),
            Bool::default(),
            Bool::default()
        )) => "[00b,00b,00b]"
    );
    // char
    macro_test_upload!(
        test_upload_vector_char,
        VectorImpl::Char(vector_build!(
            Char,
            Char::new(0i8),
            Char::new(127i8),
            Char::new(-127i8),
            Char::default()
        )) => "[0c,127c,-127c,00c]",
        VectorImpl::Char(
            Vector::<Char>::new()
        ) => "array(CHAR)",
        VectorImpl::Char(vector_build!(
            Char,
            Char::default(),
            Char::default(),
            Char::default()
        )) => "[00c,00c,00c]"
    );
    // short
    macro_test_upload!(
        test_upload_vector_short,
        VectorImpl::Short(vector_build!(
            Short,
            Short::new(0i16),
            Short::new(32767i16),
            Short::new(-32767i16),
            Short::default()
        )) => "[0h,32767h,-32767h,00h]",
        VectorImpl::Short(
            Vector::<Short>::new()
        ) => "array(SHORT)",
        VectorImpl::Short(vector_build!(
            Short,
            Short::default(),
            Short::default(),
            Short::default()
        )) => "[00h,00h,00h]"
    );
    // int
    macro_test_upload!(
        test_upload_vector_int,
        VectorImpl::Int(vector_build!(
            Int,
            Int::new(0i32),
            Int::new(2147483647i32),
            Int::new(-2147483647i32),
            Int::default()
        )) => "[0i,2147483647i,-2147483647i,00i]",
        VectorImpl::Int(
            Vector::<Int>::new()
        ) => "array(INT)",
        VectorImpl::Int(vector_build!(
            Int,
            Int::default(),
            Int::default(),
            Int::default()
        )) => "[00i,00i,00i]"
    );
    // long
    macro_test_upload!(
        test_upload_vector_long,
        VectorImpl::Long(vector_build!(
            Long,
            Long::new(0i64),
            Long::new(9223372036854775807i64),
            Long::new(-9223372036854775807i64),
            Long::default()
        )) => "[0l,9223372036854775807l,-9223372036854775807l,00l]",
        VectorImpl::Long(
            Vector::<Long>::new()
        ) => "array(LONG)",
        VectorImpl::Long(vector_build!(
            Long,
            Long::default(),
            Long::default(),
            Long::default()
        )) => "[00l,00l,00l]"
    );
    // date
    macro_test_upload!(
        test_upload_vector_date,
        VectorImpl::Date(vector_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(2022, 5, 20).unwrap(),
            Date::default()
        )) => "[1970.01.01d,2022.05.20d,00d]",
        VectorImpl::Date(
            Vector::<Date>::new()
        ) => "array(DATE)",
        VectorImpl::Date(vector_build!(
            Date,
            Date::default(),
            Date::default(),
            Date::default()
        )) => "[00d,00d,00d]"
    );
    // month
    macro_test_upload!(
        test_upload_vector_month,
        VectorImpl::Month(vector_build!(
            Month,
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(2022, 5).unwrap(),
            Month::default()
        )) => "[1970.01M,2022.05M,00M]",
        VectorImpl::Month(
            Vector::<Month>::new()
        ) => "array(MONTH)",
        VectorImpl::Month(vector_build!(
            Month,
            Month::default(),
            Month::default(),
            Month::default()
        )) => "[00M,00M,00M]"
    );
    // time
    macro_test_upload!(
        test_upload_vector_time,
        VectorImpl::Time(vector_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            Time::default()
        )) => "[00:00:00.000t,13:50:59.123t,00t]",
        VectorImpl::Time(
            Vector::<Time>::new()
        ) => "array(TIME)",
        VectorImpl::Time(vector_build!(
            Time,
            Time::default(),
            Time::default(),
            Time::default()
        )) => "[00t,00t,00t]"
    );
    // minute
    macro_test_upload!(
        test_upload_vector_minute,
        VectorImpl::Minute(vector_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(13, 50).unwrap(),
            Minute::default()
        )) => "[00:00m,13:50m,00m]",
        VectorImpl::Minute(
            Vector::<Minute>::new()
        ) => "array(MINUTE)",
        VectorImpl::Minute(vector_build!(
            Minute,
            Minute::default(),
            Minute::default(),
            Minute::default()
        )) => "[00m,00m,00m]"
    );
    // second
    macro_test_upload!(
        test_upload_vector_second,
        VectorImpl::Second(vector_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(13, 50, 59).unwrap(),
            Second::default()
        )) => "[00:00:00s,13:50:59s,00s]",
        VectorImpl::Second(
            Vector::<Second>::new()
        ) => "array(SECOND)",
        VectorImpl::Second(vector_build!(
            Second,
            Second::default(),
            Second::default(),
            Second::default()
        )) => "[00s,00s,00s]"
    );
    // datetime
    macro_test_upload!(
        test_upload_vector_datetime,
        VectorImpl::DateTime(vector_build!(
            DateTime,
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            ).unwrap(),
            DateTime::from_date_second(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Second::from_hms(13, 50, 59).unwrap()
            ).unwrap(),
            DateTime::default()
        )) => "[1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D]",
        VectorImpl::DateTime(
            Vector::<DateTime>::new()
        ) => "array(DATETIME)",
        VectorImpl::DateTime(vector_build!(
            DateTime,
            DateTime::default(),
            DateTime::default(),
            DateTime::default()
        )) => "[00D,00D,00D]"
    );
    // timestamp
    macro_test_upload!(
        test_upload_vector_timestamp,
        VectorImpl::Timestamp(vector_build!(
            Timestamp,
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            ).unwrap(),
            Timestamp::from_date_time(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap()
            ).unwrap(),
            Timestamp::default()
        )) => "[1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T]",
        VectorImpl::Timestamp(
            Vector::<Timestamp>::new()
        ) => "array(TIMESTAMP)",
        VectorImpl::Timestamp(vector_build!(
            Timestamp,
            Timestamp::default(),
            Timestamp::default(),
            Timestamp::default()
        )) => "[00T,00T,00T]"
    );
    // nanotime
    macro_test_upload!(
        test_upload_vector_nanotime,
        VectorImpl::NanoTime(vector_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            NanoTime::default()
        )) => "[00:00:00.000000000n,13:50:59.123456789n,00n]",
        VectorImpl::NanoTime(
            Vector::<NanoTime>::new()
        ) => "array(NANOTIME)",
        VectorImpl::NanoTime(vector_build!(
            NanoTime,
            NanoTime::default(),
            NanoTime::default(),
            NanoTime::default()
        )) => "[00n,00n,00n]"
    );
    // nanotimestamp
    macro_test_upload!(
        test_upload_vector_nanotimestamp,
        VectorImpl::NanoTimestamp(vector_build!(
            NanoTimestamp,
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            ).unwrap(),
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            ).unwrap(),
            NanoTimestamp::default()
        )) => "[1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N]",
        VectorImpl::NanoTimestamp(
            Vector::<NanoTimestamp>::new()
        ) => "array(NANOTIMESTAMP)",
        VectorImpl::NanoTimestamp(vector_build!(
            NanoTimestamp,
            NanoTimestamp::default(),
            NanoTimestamp::default(),
            NanoTimestamp::default()
        )) => "[00N,00N,00N]"
    );
    // datehour
    macro_test_upload!(
        test_upload_vector_datehour,
        VectorImpl::DateHour(vector_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DateHour::default()
        )) => "datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL])",
        VectorImpl::DateHour(
            Vector::<DateHour>::new()
        ) => "array(DATEHOUR)",
        VectorImpl::DateHour(vector_build!(
            DateHour,
            DateHour::default(),
            DateHour::default(),
            DateHour::default()
        )) => "[datehour(NULL),datehour(NULL),datehour(NULL)]"
    );
    // float
    // todo:nan inf
    macro_test_upload!(
        test_upload_vector_float,
        VectorImpl::Float(vector_build!(
            Float,
            Float::new(0.0f32),
            Float::new(3.14f32),
            Float::default()
        )) => "[0.0f,3.14f,00f]",
        VectorImpl::Float(
            Vector::<Float>::new()
        ) => "array(FLOAT)",
        VectorImpl::Float(vector_build!(
            Float,
            Float::default(),
            Float::default(),
            Float::default()
        )) => "[00f,00f,00f]"
    );
    // double
    macro_test_upload!(
        test_upload_vector_double,
        VectorImpl::Double(vector_build!(
            Double,
            Double::new(0.0f64),
            Double::new(3.14f64),
            Double::default()
        )) => "[0.0F,3.14F,00F]",
        VectorImpl::Double(
            Vector::<Double>::new()
        ) => "array(DOUBLE)",
        VectorImpl::Double(vector_build!(
            Double,
            Double::default(),
            Double::default(),
            Double::default()
        )) => "[00F,00F,00F]"
    );
    // symbol
    macro_test_upload!(
        test_upload_vector_symbol,
        VectorImpl::Symbol(vector_build!(
            Symbol,
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::default()
        )) => "symbol([\"abc!@#中文 123\",\"\"])",
        VectorImpl::Symbol(
            Vector::<Symbol>::new()
        ) => "array(SYMBOL)",
        VectorImpl::Symbol(vector_build!(
            Symbol,
            Symbol::default(),
            Symbol::default(),
            Symbol::default()
        )) => "symbol([\"\",\"\",\"\"])"
    );
    // string
    macro_test_upload!(
        test_upload_vector_string,
        VectorImpl::String(vector_build!(
            DolphinString,
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::default()
        )) => "[\"abc!@#中文 123\",\"\"]",
        VectorImpl::String(
            Vector::<DolphinString>::new()
        ) => "array(STRING)",
        VectorImpl::String(vector_build!(
            DolphinString,
            DolphinString::default(),
            DolphinString::default(),
            DolphinString::default()
        )) => "[\"\",\"\",\"\"]"
    );
    // blob
    macro_test_upload!(
        test_upload_vector_blob,
        VectorImpl::Blob(vector_build!(
            Blob,
            Blob::new("abc!@#中文 123".as_bytes().to_vec()),
            Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()),
            Blob::default()
        )) => "[blob(\"abc!@#中文 123\"),blob(fromUTF8(\"abc!@#中文 123\",\"gbk\")),blob(\"\")]",
        VectorImpl::Blob(
            Vector::<Blob>::new()
        ) => "array(BLOB)",
        VectorImpl::Blob(vector_build!(
            Blob,
            Blob::default(),
            Blob::default(),
            Blob::default()
        )) => "[blob(\"\"),blob(\"\"),blob(\"\")]"
    );
    // decimal32
    macro_test_upload!(
        test_upload_vector_decimal32,
        VectorImpl::Decimal32(vector_build!(
            Decimal32,
            Decimal32::from_raw(314i32, 2).unwrap(),
            Decimal32::default()
        )) => "decimal32([\"3.14\",NULL],2)",
        VectorImpl::Decimal32(
            Vector::<Decimal32>::new()
        ) => "array(DECIMAL32(0))",
        VectorImpl::Decimal32(vector_build!(
            Decimal32,
            Decimal32::default(),
            Decimal32::default(),
            Decimal32::default()
        )) => "[decimal32(NULL,0),decimal32(NULL,0),decimal32(NULL,0)]"
    );
    // decimal64
    macro_test_upload!(
        test_upload_vector_decimal64,
        VectorImpl::Decimal64(vector_build!(
            Decimal64,
            Decimal64::from_raw(314i64, 2).unwrap(),
            Decimal64::default()
        )) => "decimal64([\"3.14\",NULL],2)",
        VectorImpl::Decimal64(
            Vector::<Decimal64>::new()
        ) => "array(DECIMAL64(0))",
        VectorImpl::Decimal64(vector_build!(
            Decimal64,
            Decimal64::default(),
            Decimal64::default(),
            Decimal64::default()
        )) => "[decimal64(NULL,0),decimal64(NULL,0),decimal64(NULL,0)]"
    );
    // decimal128
    macro_test_upload!(
        test_upload_vector_decimal128,
        VectorImpl::Decimal128(vector_build!(
            Decimal128,
            Decimal128::from_raw(314i128, 2).unwrap(),
            Decimal128::default()
        )) => "decimal128([\"3.14\",NULL],2)",
        VectorImpl::Decimal128(
            Vector::<Decimal128>::new()
        ) => "array(DECIMAL128(0))",
        VectorImpl::Decimal128(vector_build!(
            Decimal128,
            Decimal128::default(),
            Decimal128::default(),
            Decimal128::default()
        )) => "[decimal128(NULL,0),decimal128(NULL,0),decimal128(NULL,0)]"
    );
    // any
    macro_test_upload!(
        test_upload_vector_any,
        VectorImpl::Any(vector_build!(
            Any,
            Any::new(Int::new(1i32).into()),
            Any::new(Int::new(2i32).into()),
            Any::new(Int::new(3i32).into())
        )) => "(1,2,3)",
        VectorImpl::Any(
            Vector::<Any>::new()
        ) => "array(ANY)",
        VectorImpl::Any(vector_build!(
            Any,
            Any::new(Int::new(1i32).into()),
            Any::new(vector_build!(Int,Int::new(1i32)).into()),
            Any::new(SetImpl::Int(set_build!(Int,Int::new(1i32))).into()),
            Any::new(DictionaryImpl::Int(dictionary_build!(Int,Int::new(1i32) => Int::new(1i32))).into()),
            Any::new(table_build!(
                String::from("a") => vector_build!(
                    Int,
                    Int::new(1i32)
                )
            ).into())
        )) => "(1,[1],set([1]),dict([1],[1]),table([1] as `a))"
    );
}

// todo:matrix

mod test_upload_set {
    use super::*;

    // char
    macro_test_upload!(
        test_upload_set_char,
        SetImpl::Char(set_build!(
            Char,
            Char::new(0i8),
            Char::new(127i8),
            Char::new(-127i8),
            Char::default()
        )) => "set(0c 127c -127c 00c)",
        SetImpl::Char(
            Set::<Char>::new()
        ) => "set(array(CHAR))"
    );
    // short
    macro_test_upload!(
        test_upload_set_short,
        SetImpl::Short(set_build!(
            Short,
            Short::new(0i16),
            Short::new(32767i16),
            Short::new(-32767i16),
            Short::default()
        )) => "set(0h 32767h -32767h 00h)",
        SetImpl::Short(
            Set::<Short>::new()
        ) => "set(array(SHORT))"
    );
    // int
    macro_test_upload!(
        test_upload_set_int,
        SetImpl::Int(set_build!(
            Int,
            Int::new(0i32),
            Int::new(2147483647i32),
            Int::new(-2147483647i32),
            Int::default()
        )) => "set(0i 2147483647i -2147483647i 00i)",
        SetImpl::Int(
            Set::<Int>::new()
        ) => "set(array(INT))"
    );
    // long
    macro_test_upload!(
        test_upload_set_long,
        SetImpl::Long(set_build!(
            Long,
            Long::new(0i64),
            Long::new(9223372036854775807i64),
            Long::new(-9223372036854775807i64),
            Long::default()
        )) => "set(0l 9223372036854775807l -9223372036854775807l 00l)",
        SetImpl::Long(
            Set::<Long>::new()
        ) => "set(array(LONG))"
    );
    // date
    macro_test_upload!(
        test_upload_set_date,
        SetImpl::Date(set_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(2022, 5, 20).unwrap(),
            Date::default()
        )) => "set(1970.01.01d 2022.05.20d 00d)",
        SetImpl::Date(
            Set::<Date>::new()
        ) => "set(array(DATE))"
    );
    // month
    macro_test_upload!(
        test_upload_set_month,
        SetImpl::Month(set_build!(
            Month,
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(2022, 5).unwrap(),
            Month::default()
        )) => "set(1970.01M 2022.05M 00M)",
        SetImpl::Month(
            Set::<Month>::new()
        ) => "set(array(MONTH))"
    );
    // time
    macro_test_upload!(
        test_upload_set_time,
        SetImpl::Time(set_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            Time::default()
        )) => "set(00:00:00.000t 13:50:59.123t 00t)",
        SetImpl::Time(
            Set::<Time>::new()
        ) => "set(array(TIME))"
    );
    // minute
    macro_test_upload!(
        test_upload_set_minute,
        SetImpl::Minute(set_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(13, 50).unwrap(),
            Minute::default()
        )) => "set(00:00m 13:50m 00m)",
        SetImpl::Minute(
            Set::<Minute>::new()
        ) => "set(array(MINUTE))"
    );
    // second
    macro_test_upload!(
        test_upload_set_second,
        SetImpl::Second(set_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(13, 50, 59).unwrap(),
            Second::default()
        )) => "set(00:00:00 13:50:59 00s)",
        SetImpl::Second(
            Set::<Second>::new()
        ) => "set(array(SECOND))"
    );
    // datetime
    macro_test_upload!(
        test_upload_set_datetime,
        SetImpl::DateTime(set_build!(
            DateTime,
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            )
            .unwrap(),
            DateTime::from_date_second(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Second::from_hms(13, 50, 59).unwrap()
            )
            .unwrap(),
            DateTime::default()
        )) => "set(1970.01.01T00:00:00D 2022.05.20T13:50:59D 00D)",
        SetImpl::DateTime(
            Set::<DateTime>::new()
        ) => "set(array(DATETIME))"
    );
    // timestamp
    macro_test_upload!(
        test_upload_set_timestamp,
        SetImpl::Timestamp(set_build!(
            Timestamp,
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            )
            .unwrap(),
            Timestamp::from_date_time(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap()
            )
            .unwrap(),
            Timestamp::default()
        )) => "set(1970.01.01T00:00:00.000T 2022.05.20T13:50:59.123T 00T)",
        SetImpl::Timestamp(
            Set::<Timestamp>::new()
        ) => "set(array(TIMESTAMP))"
    );
    // nanotime
    macro_test_upload!(
        test_upload_set_nanotime,
        SetImpl::NanoTime(set_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            NanoTime::default()
        )) => "set(00:00:00.000000000n 13:50:59.123456789n 00n)",
        SetImpl::NanoTime(
            Set::<NanoTime>::new()
        ) => "set(array(NANOTIME))"
    );
    // nanotimestamp
    macro_test_upload!(
        test_upload_set_nanotimestamp,
        SetImpl::NanoTimestamp(set_build!(
            NanoTimestamp,
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            )
            .unwrap(),
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            )
            .unwrap(),
            NanoTimestamp::default()
        )) => "set(1970.01.01T00:00:00.000000000N 2022.05.20T13:50:59.123456789N 00N)",
        SetImpl::NanoTimestamp(
            Set::<NanoTimestamp>::new()
        ) => "set(array(NANOTIMESTAMP))"
    );
    // datehour
    macro_test_upload!(
        test_upload_set_datehour,
        SetImpl::DateHour(set_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DateHour::default()
        )) => "set([datehour(\"1970.01.01T00\"),datehour(\"2022.05.20T13\"),datehour(NULL)])",
        SetImpl::DateHour(
            Set::<DateHour>::new()
        ) => "set(array(DATEHOUR))"
    );
    // float
    macro_test_upload!(
        test_upload_set_float,
        SetImpl::Float(set_build!(
            Float,
            Float::new(3.14f32),
            Float::new(3.15f32),
            Float::default()
        )) => "set(3.14f 3.15f 00f)",
        SetImpl::Float(
            Set::<Float>::new()
        ) => "set(array(FLOAT))"
    );
    // double
    macro_test_upload!(
        test_upload_set_double,
        SetImpl::Double(set_build!(
            Double,
            Double::new(3.14f64),
            Double::new(3.15f64),
            Double::default()
        )) => "set(3.14F 3.15F 00F)",
        SetImpl::Double(
            Set::<Double>::new()
        ) => "set(array(DOUBLE))"
    );
    // symbol
    macro_test_upload!(
        test_upload_set_symbol,
        SetImpl::Symbol(set_build!(
            Symbol,
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::new(String::from("abc!@#中文 124")),
            Symbol::default()
        )) => "set(symbol(\"abc!@#中文 123\" \"abc!@#中文 124\" \"\"))",
        SetImpl::Symbol(
            Set::<Symbol>::new()
        ) => "set(array(SYMBOL))"
    );
    // string
    macro_test_upload!(
        test_upload_set_string,
        SetImpl::String(set_build!(
            DolphinString,
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("abc!@#中文 124")),
            DolphinString::default()
        )) => "set(\"abc!@#中文 123\" \"abc!@#中文 124\" \"\")",
        SetImpl::String(
            Set::<DolphinString>::new()
        ) => "set(array(STRING))"
    );
    // Blob
    // macro_test_upload!(
    //     test_upload_set_Blob,
    //     SetImpl::Blob(set_build!(
    //         Blob,
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         Blob::new("abc!@#中文 124".as_bytes().to_vec()),
    //         Blob::default()
    //     )) => "set(blob(\"abc!@#中文 123\" \"abc!@#中文 124\" \"\"))",
    //     SetImpl::Blob(
    //         Set::<Blob>::new()
    //     ) => "set(array(BLOB))"
    // );
}

mod test_upload_dictionary {
    use super::*;

    // char->bool
    macro_test_upload!(
        test_upload_dictionary_char_bool,
        DictionaryImpl::Char(dictionary_build!(
            Char,
            Char::new(0i8) => Bool::new(true),
            Char::new(127i8) => Bool::new(true),
            Char::new(-127i8) => Bool::new(false),
            Char::default() => Bool::default()
        )) => "dict([0c,127c,-127c,00c],[true,true,false,00b])"
    );
    // short->bool
    macro_test_upload!(
        test_upload_dictionary_short_bool,
        DictionaryImpl::Short(dictionary_build!(
            Short,
            Short::new(0i16) => Bool::new(true),
            Short::new(32767i16) => Bool::new(true),
            Short::new(-32767i16) => Bool::new(false),
            Short::default() => Bool::default()
        )) => "dict([0h,32767h,-32767h,00h],[true,true,false,00b])"
    );
    // int->bool
    macro_test_upload!(
        test_upload_dictionary_int_bool,
        DictionaryImpl::Int(dictionary_build!(
            Int,
            Int::new(0i32) => Bool::new(true),
            Int::new(2147483647i32) => Bool::new(true),
            Int::new(-2147483647i32) => Bool::new(false),
            Int::default() => Bool::default()
        )) => "dict([0i,2147483647i,-2147483647i,00i],[true,true,false,00b])"
    );
    // long->bool
    macro_test_upload!(
        test_upload_dictionary_long_bool,
        DictionaryImpl::Long(dictionary_build!(
            Long,
            Long::new(0i64) => Bool::new(true),
            Long::new(9223372036854775807i64) => Bool::new(true),
            Long::new(-9223372036854775807i64) => Bool::new(false),
            Long::default() => Bool::default()
        )) => "dict([0l,9223372036854775807l,-9223372036854775807l,00l],[true,true,false,00b])"
    );
    // date->bool
    macro_test_upload!(
        test_upload_dictionary_date_bool,
        DictionaryImpl::Date(dictionary_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap() => Bool::new(true),
            Date::from_ymd(2022, 5, 20).unwrap() => Bool::new(false),
            Date::default() => Bool::default()
        )) => "dict([1970.01.01d,2022.05.20d,00d],[true,false,00b])"
    );
    // month->bool
    macro_test_upload!(
        test_upload_dictionary_month_bool,
        DictionaryImpl::Month(dictionary_build!(
            Month,
            Month::from_ym(1970, 1).unwrap() => Bool::new(true),
            Month::from_ym(2022, 5).unwrap() => Bool::new(false),
            Month::default() => Bool::default()
        )) => "dict([1970.01M,2022.05M,00M],[true,false,00b])"
    );
    // time->bool
    macro_test_upload!(
        test_upload_dictionary_time_bool,
        DictionaryImpl::Time(dictionary_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap() => Bool::new(true),
            Time::from_hms_milli(13, 50, 59, 123).unwrap() => Bool::new(false),
            Time::default() => Bool::default()
        )) => "dict([00:00:00.000t,13:50:59.123t,00t],[true,false,00b])"
    );
    // minute->bool
    macro_test_upload!(
        test_upload_dictionary_minute_bool,
        DictionaryImpl::Minute(dictionary_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap() => Bool::new(true),
            Minute::from_hm(13, 50).unwrap() => Bool::new(false),
            Minute::default() => Bool::default()
        )) => "dict([00:00m,13:50m,00m],[true,false,00b])"
    );
    // second->bool
    macro_test_upload!(
        test_upload_dictionary_second_bool,
        DictionaryImpl::Second(dictionary_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap() => Bool::new(true),
            Second::from_hms(13, 50, 59).unwrap() => Bool::new(false),
            Second::default() => Bool::default()
        )) => "dict([00:00:00s,13:50:59s,00s],[true,false,00b])"
    );
    // datetime->bool
    macro_test_upload!(
        test_upload_dictionary_datetime_bool,
        DictionaryImpl::DateTime(dictionary_build!(
            DateTime,
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            ).unwrap() => Bool::new(true),
            DateTime::from_date_second(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Second::from_hms(13, 50, 59).unwrap()
            ).unwrap() => Bool::new(false),
            DateTime::default() => Bool::default()
        )) => "dict([1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D],[true,false,00b])"
    );
    // timestamp->bool
    macro_test_upload!(
        test_upload_dictionary_timestamp_bool,
        DictionaryImpl::Timestamp(dictionary_build!(
            Timestamp,
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            ).unwrap() => Bool::new(true),
            Timestamp::from_date_time(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap()
            ).unwrap() => Bool::new(false),
            Timestamp::default() => Bool::default()
        )) => "dict([1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T],[true,false,00b])"
    );
    // nanotime->bool
    macro_test_upload!(
        test_upload_dictionary_nanotime_bool,
        DictionaryImpl::NanoTime(dictionary_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap() => Bool::new(true),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap() => Bool::new(false),
            NanoTime::default() => Bool::default()
        )) => "dict([00:00:00.000000000n,13:50:59.123456789,00n],[true,false,00b])"
    );
    // nanotimestamp->bool
    macro_test_upload!(
        test_upload_dictionary_nanotimestamp_bool,
        DictionaryImpl::NanoTimestamp(dictionary_build!(
            NanoTimestamp,
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            ).unwrap() => Bool::new(true),
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            ).unwrap() => Bool::new(false),
            NanoTimestamp::default() => Bool::default()
        )) => "dict([1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N],[true,false,00b])"
    );
    // datehour->bool
    macro_test_upload!(
        test_upload_dictionary_datehour_bool,
        DictionaryImpl::DateHour(dictionary_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap() => Bool::new(true),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap() => Bool::new(false),
            DateHour::default() => Bool::default()
        )) => "dict(datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]),[true,false,00b])"
    );
    // float->bool
    macro_test_upload!(
        test_upload_dictionary_float_bool,
        DictionaryImpl::Float(dictionary_build!(
            Float,
            Float::new(3.14f32) => Bool::new(true),
            Float::new(3.15f32) => Bool::new(false),
            Float::default() => Bool::default()
        )) => "dict([3.14f,3.15f,00f],[true,false,00b])"
    );
    // double->bool
    macro_test_upload!(
        test_upload_dictionary_double_bool,
        DictionaryImpl::Double(dictionary_build!(
            Double,
            Double::new(3.14f64) => Bool::new(true),
            Double::new(3.15f64) => Bool::new(false),
            Double::default() => Bool::default()
        )) => "dict([3.14F,3.15F,00F],[true,false,00b])"
    );
    // symbol->bool
    macro_test_upload!(
        test_upload_dictionary_symbol_bool,
        DictionaryImpl::Symbol(dictionary_build!(
            Symbol,
            Symbol::new(String::from("abc!@#中文 123")) => Bool::new(true),
            Symbol::new(String::from("abc!@#中文 124")) => Bool::new(false),
            Symbol::default() => Bool::default()
        )) => "dict(symbol([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]),[true,false,00b])"
    );
    // string->bool
    macro_test_upload!(
        test_upload_dictionary_string_bool,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("abc!@#中文 123")) => Bool::new(true),
            DolphinString::new(String::from("abc!@#中文 124")) => Bool::new(false),
            DolphinString::default() => Bool::default()
        )) => "dict([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"],[true,false,00b])"
    );
    // blob->bool
    // macro_test_upload!(
    //     test_upload_dictionary_blob_bool,
    //     DictionaryImpl::Blob(dictionary_build!(
    //         Blob,
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()) => Bool::new(true),
    //         Blob::new("abc!@#中文 124".as_bytes().to_vec()) => Bool::new(false),
    //         Blob::default() => Bool::default()
    //     )) => "dict([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"],[true,false,00b])"
    // );
    // string->char
    macro_test_upload!(
        test_upload_dictionary_string_char,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Char::new(127i8),
            DolphinString::new(String::from("2")) => Char::new(-127i8),
            DolphinString::default() => Char::default()
        )) => "dict([\"1\",\"2\",\"\"],[127c,-127c,00c])"
    );
    // string->short
    macro_test_upload!(
        test_upload_dictionary_string_short,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Short::new(32767i16),
            DolphinString::new(String::from("2")) => Short::new(-32767i16),
            DolphinString::default() => Short::default()
        )) => "dict([\"1\",\"2\",\"\"],[32767h,-32767h,00h])"
    );
    // string->int
    macro_test_upload!(
        test_upload_dictionary_string_int,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Int::new(2147483647i32),
            DolphinString::new(String::from("2")) => Int::new(-2147483647i32),
            DolphinString::default() => Int::default()
        )) => "dict([\"1\",\"2\",\"\"],[2147483647i,-2147483647i,00i])"
    );
    // string->long
    macro_test_upload!(
        test_upload_dictionary_string_long,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Long::new(9223372036854775807i64),
            DolphinString::new(String::from("2")) => Long::new(-9223372036854775807i64),
            DolphinString::default() => Long::default()
        )) => "dict([\"1\",\"2\",\"\"],[9223372036854775807l,-9223372036854775807l,00l])"
    );
    // string->date
    macro_test_upload!(
        test_upload_dictionary_string_date,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Date::from_ymd(1970, 1, 1).unwrap(),
            DolphinString::new(String::from("2")) => Date::from_ymd(2022, 5, 20).unwrap(),
            DolphinString::default() => Date::default()
        )) => "dict([\"1\",\"2\",\"\"],[1970.01.01d,2022.05.20d,00d])"
    );
    // string->month
    macro_test_upload!(
        test_upload_dictionary_string_month,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Month::from_ym(1970, 1).unwrap(),
            DolphinString::new(String::from("2")) => Month::from_ym(2022, 5).unwrap(),
            DolphinString::default() => Month::default()
        )) => "dict([\"1\",\"2\",\"\"],[1970.01M,2022.05M,00M])"
    );
    // string->time
    macro_test_upload!(
        test_upload_dictionary_string_time,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            DolphinString::new(String::from("2")) => Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            DolphinString::default() => Time::default()
        )) => "dict([\"1\",\"2\",\"\"],[00:00:00.000t,13:50:59.123t,00t])"
    );
    // string->minute
    macro_test_upload!(
        test_upload_dictionary_string_minute,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Minute::from_hm(0, 0).unwrap(),
            DolphinString::new(String::from("2")) => Minute::from_hm(13, 50).unwrap(),
            DolphinString::default() => Minute::default()
        )) => "dict([\"1\",\"2\",\"\"],[00:00m,13:50m,00m])"
    );
    // string->second
    macro_test_upload!(
        test_upload_dictionary_string_second,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Second::from_hms(0, 0, 0).unwrap(),
            DolphinString::new(String::from("2")) => Second::from_hms(13, 50, 59).unwrap(),
            DolphinString::default() => Second::default()
        )) => "dict([\"1\",\"2\",\"\"],[00:00:00s,13:50:59s,00s])"
    );
    // string->datetime
    macro_test_upload!(
        test_upload_dictionary_string_datetime,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            ).unwrap(),
            DolphinString::new(String::from("2")) => DateTime::from_date_second(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Second::from_hms(13, 50, 59).unwrap()
            ).unwrap(),
            DolphinString::default() => DateTime::default()
        )) => "dict([\"1\",\"2\",\"\"],[1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D])"
    );
    // string->timestamp
    macro_test_upload!(
        test_upload_dictionary_string_timestamp,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            ).unwrap(),
            DolphinString::new(String::from("2")) => Timestamp::from_date_time(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap()
            ).unwrap(),
            DolphinString::default() => Timestamp::default()
        )) => "dict([\"1\",\"2\",\"\"],[1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T])"
    );
    // string->nanotime
    macro_test_upload!(
        test_upload_dictionary_string_nanotime,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            DolphinString::new(String::from("2")) => NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            DolphinString::default() => NanoTime::default()
        )) => "dict([\"1\",\"2\",\"\"],[00:00:00.000000000n,13:50:59.123456789n,00n])"
    );
    // string->nanotimestamp
    macro_test_upload!(
        test_upload_dictionary_string_nanotimestamp,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            ).unwrap(),
            DolphinString::new(String::from("2")) => NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            ).unwrap(),
            DolphinString::default() => NanoTimestamp::default()
        )) => "dict([\"1\",\"2\",\"\"],[1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N])"
    );
    // string->datehour
    macro_test_upload!(
        test_upload_dictionary_string_datehour,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DolphinString::new(String::from("2")) => DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DolphinString::default() => DateHour::default()
        )) => "dict([\"1\",\"2\",\"\"],datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]))"
    );
    // string->float
    macro_test_upload!(
        test_upload_dictionary_string_float,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Float::new(3.14f32),
            DolphinString::new(String::from("2")) => Float::new(3.15f32),
            DolphinString::default() => Float::default()
        )) => "dict([\"1\",\"2\",\"\"],[3.14f,3.15f,00f])"
    );
    // string->double
    macro_test_upload!(
        test_upload_dictionary_string_double,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Double::new(3.14f64),
            DolphinString::new(String::from("2")) => Double::new(3.15f64),
            DolphinString::default() => Double::default()
        )) => "dict([\"1\",\"2\",\"\"],[3.14F,3.15F,00F])"
    );
    // string->symbol
    macro_test_upload!(
        test_upload_dictionary_string_symbol,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Symbol::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("2")) => Symbol::new(String::from("abc!@#中文 124")),
            DolphinString::default() => Symbol::default()
        )) => "dict([\"1\",\"2\",\"\"],symbol([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]))"
    );
    // string->string
    macro_test_upload!(
        test_upload_dictionary_string_string,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("2")) => DolphinString::new(String::from("abc!@#中文 124")),
            DolphinString::default() => DolphinString::default()
        )) => "dict([\"1\",\"2\",\"\"],[\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"])"
    );
    // string->blob
    // macro_test_upload!(
    //     test_upload_dictionary_string_blob,
    //     DictionaryImpl::String(dictionary_build!(
    //         DolphinString,
    //         DolphinString::new(String::from("1")) => Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         DolphinString::new(String::from("2")) => Blob::new("abc!@#中文 124".as_bytes().to_vec()),
    //         DolphinString::default() => Blob::default()
    //     )) => "dict([\"1\",\"2\",\"\"],blob([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]))"
    // );
    // string->decimal32
    macro_test_upload!(
        test_upload_dictionary_string_decimal32,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Decimal32::from_raw(314i32, 2).unwrap(),
            DolphinString::new(String::from("2")) => Decimal32::from_raw(315i32, 2).unwrap(),
            DolphinString::default() => Decimal32::default()
        )) => "dict([\"1\",\"2\",\"\"],decimal32([\"3.14\",\"3.15\",NULL],2))"
    );
    // string->decimal64
    macro_test_upload!(
        test_upload_dictionary_string_decimal64,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Decimal64::from_raw(314i64, 2).unwrap(),
            DolphinString::new(String::from("2")) => Decimal64::from_raw(315i64, 2).unwrap(),
            DolphinString::default() => Decimal64::default()
        )) => "dict([\"1\",\"2\",\"\"],decimal64([\"3.14\",\"3.15\",NULL],2))"
    );
    // string->decimal128
    macro_test_upload!(
        test_upload_dictionary_string_decimal128,
        DictionaryImpl::String(dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Decimal128::from_raw(314i128, 2).unwrap(),
            DolphinString::new(String::from("2")) => Decimal128::from_raw(315i128, 2).unwrap(),
            DolphinString::default() => Decimal128::default()
        )) => "dict([\"1\",\"2\",\"\"],decimal128([\"3.14\",\"3.15\",NULL],2))"
    );
    // string->any
    macro_test_upload!(
        test_upload_dictionary_string_any,
        DictionaryImpl::String(dictionary_build_any!(
            DolphinString,
            DolphinString::new(String::from("1")) => Any::new(Int::new(1i32).into()),
            DolphinString::new(String::from("2")) => Any::new(vector_build!(Int,Int::new(1i32)).into()),
            DolphinString::new(String::from("3")) => Any::new(SetImpl::Int(set_build!(Int,Int::new(1i32))).into()),
            DolphinString::new(String::from("4")) => Any::new(DictionaryImpl::Int(dictionary_build!(Int,Int::new(1i32) => Int::new(1i32))).into()),
            DolphinString::new(String::from("5")) => Any::new(table_build!(
                String::from("a") => vector_build!(
                    Int,
                    Int::new(1i32)
                )
            ).into())
        )) => "dict([\"1\",\"2\",\"3\",\"4\",\"5\"],(1,[1],set([1]),dict([1],[1]),table([1] as `a)))",
        DictionaryImpl::String(Dictionary::<DolphinString>::new()) => "dict([]$STRING,[]$ANY)"
    );
}

mod test_upload_table {
    use super::*;

    // bool
    macro_test_upload!(
        test_upload_table_bool,
        table_build!(
            String::from("a") => vector_build!(
                Bool,
                Bool::new(true),
                Bool::new(false),
                Bool::default()
            )
        ) => "table([true,false,00b] as `a)",
        table_build!(
            String::from("a") => Vector::<Bool>::new()
        ) => "table([]$BOOL as `a)"
    );
    // char
    macro_test_upload!(
        test_upload_table_char,
        table_build!(
            String::from("a") => vector_build!(
                Char,
                Char::new(127i8),
                Char::new(-127i8),
                Char::new(0i8),
                Char::default()
            )
        ) => "table([127c,-127c,0c,00c] as `a)",
        table_build!(
            String::from("a") => Vector::<Char>::new()
        ) => "table([]$CHAR as `a)"
    );
    // short
    macro_test_upload!(
        test_upload_table_short,
        table_build!(
            String::from("a") => vector_build!(
                Short,
                Short::new(32767i16),
                Short::new(-32767i16),
                Short::new(0i16),
                Short::default()
            )
        ) => "table([32767h,-32767h,0h,00h] as `a)",
        table_build!(
            String::from("a") => Vector::<Short>::new()
        ) => "table([]$SHORT as `a)"
    );
    // int
    macro_test_upload!(
        test_upload_table_int,
        table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        ) => "table([2147483647i,-2147483647i,0i,00i] as `a)",
        table_build!(
            String::from("a") => Vector::<Int>::new()
        ) => "table([]$INT as `a)"
    );
    // long
    macro_test_upload!(
        test_upload_table_long,
        table_build!(
            String::from("a") => vector_build!(
                Long,
                Long::new(9223372036854775807i64),
                Long::new(-9223372036854775807i64),
                Long::new(0i64),
                Long::default()
            )
        ) => "table([9223372036854775807l,-9223372036854775807l,0l,00l] as `a)",
        table_build!(
            String::from("a") => Vector::<Long>::new()
        ) => "table([]$LONG as `a)"
    );
    // date
    macro_test_upload!(
        test_upload_table_date,
        table_build!(
            String::from("a") => vector_build!(
                Date,
                Date::from_ymd(1970, 1, 1).unwrap(),
                Date::from_ymd(2022, 5, 20).unwrap(),
                Date::default()
            )
        ) => "table([1970.01.01d,2022.05.20d,00d] as `a)",
        table_build!(
            String::from("a") => Vector::<Date>::new()
        ) => "table([]$DATE as `a)"
    );
    // month
    macro_test_upload!(
        test_upload_table_month,
        table_build!(
            String::from("a") => vector_build!(
                Month,
                Month::from_ym(1970, 1).unwrap(),
                Month::from_ym(2022, 5).unwrap(),
                Month::default()
            )
        ) => "table([1970.01M,2022.05M,00M] as `a)",
        table_build!(
            String::from("a") => Vector::<Month>::new()
        ) => "table([]$MONTH as `a)"
    );
    // time
    macro_test_upload!(
        test_upload_table_time,
        table_build!(
            String::from("a") => vector_build!(
                Time,
                Time::from_hms_milli(0, 0, 0, 0).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap(),
                Time::default()
            )
        ) => "table([00:00:00.000t,13:50:59.123t,00t] as `a)",
        table_build!(
            String::from("a") => Vector::<Time>::new()
        ) => "table([]$TIME as `a)"
    );
    // minute
    macro_test_upload!(
        test_upload_table_minute,
        table_build!(
            String::from("a") => vector_build!(
                Minute,
                Minute::from_hm(0, 0).unwrap(),
                Minute::from_hm(13, 50).unwrap(),
                Minute::default()
            )
        ) => "table([00:00m,13:50m,00m] as `a)",
        table_build!(
            String::from("a") => Vector::<Minute>::new()
        ) => "table([]$MINUTE as `a)"
    );
    // second
    macro_test_upload!(
        test_upload_table_second,
        table_build!(
            String::from("a") => vector_build!(
                Second,
                Second::from_hms(0, 0, 0).unwrap(),
                Second::from_hms(13, 50, 59).unwrap(),
                Second::default()
            )
        ) => "table([00:00:00s,13:50:59s,00s] as `a)",
        table_build!(
            String::from("a") => Vector::<Second>::new()
        ) => "table([]$SECOND as `a)"
    );
    // datetime
    macro_test_upload!(
        test_upload_table_datetime,
        table_build!(
            String::from("a") => vector_build!(
                DateTime,
                DateTime::from_date_second(
                    Date::from_ymd(1970, 1, 1).unwrap(),
                    Second::from_hms(0, 0, 0).unwrap()
                ).unwrap(),
                DateTime::from_date_second(
                    Date::from_ymd(2022, 5, 20).unwrap(),
                    Second::from_hms(13, 50, 59).unwrap()
                ).unwrap(),
                DateTime::default()
            )
        ) => "table([1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D] as `a)",
        table_build!(
            String::from("a") => Vector::<DateTime>::new()
        ) => "table([]$DATETIME as `a)"
    );
    // timestamp
    macro_test_upload!(
        test_upload_table_timestamp,
        table_build!(
            String::from("a") => vector_build!(
                Timestamp,
                Timestamp::from_date_time(
                    Date::from_ymd(1970, 1, 1).unwrap(),
                    Time::from_hms_milli(0, 0, 0, 0).unwrap()
                ).unwrap(),
                Timestamp::from_date_time(
                    Date::from_ymd(2022, 5, 20).unwrap(),
                    Time::from_hms_milli(13, 50, 59, 123).unwrap()
                ).unwrap(),
                Timestamp::default()
            )
        ) => "table([1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T] as `a)",
        table_build!(
            String::from("a") => Vector::<Timestamp>::new()
        ) => "table([]$TIMESTAMP as `a)"
    );
    // nanotime
    macro_test_upload!(
        test_upload_table_nanotime,
        table_build!(
            String::from("a") => vector_build!(
                NanoTime,
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
                NanoTime::default()
            )
        ) => "table([00:00:00.000000000n,13:50:59.123456789n,00n] as `a)",
        table_build!(
            String::from("a") => Vector::<NanoTime>::new()
        ) => "table([]$NANOTIME as `a)"
    );
    // nanotimestamp
    macro_test_upload!(
        test_upload_table_nanotimestamp,
        table_build!(
            String::from("a") => vector_build!(
                NanoTimestamp,
                NanoTimestamp::from_date_nanotime(
                    Date::from_ymd(1970, 1, 1).unwrap(),
                    NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
                ).unwrap(),
                NanoTimestamp::from_date_nanotime(
                    Date::from_ymd(2022, 5, 20).unwrap(),
                    NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
                ).unwrap(),
                NanoTimestamp::default()
            )
        ) => "table([1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N] as `a)",
        table_build!(
            String::from("a") => Vector::<NanoTimestamp>::new()
        ) => "table([]$NANOTIMESTAMP as `a)"
    );
    // datehour
    macro_test_upload!(
        test_upload_table_datehour,
        table_build!(
            String::from("a") => vector_build!(
                DateHour,
                DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
                DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
                DateHour::default()
            )
        ) => "table(datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]) as `a)",
        table_build!(
            String::from("a") => Vector::<DateHour>::new()
        ) => "table([]$DATEHOUR as `a)"
    );
    // float
    macro_test_upload!(
        test_upload_table_float,
        table_build!(
            String::from("a") => vector_build!(
                Float,
                Float::new(3.14f32),
                Float::new(3.15f32),
                Float::default()
            )
        ) => "table([3.14f,3.15f,00f] as `a)",
        table_build!(
            String::from("a") => Vector::<Float>::new()
        ) => "table([]$FLOAT as `a)"
    );
    // double
    macro_test_upload!(
        test_upload_table_double,
        table_build!(
            String::from("a") => vector_build!(
                Double,
                Double::new(3.14f64),
                Double::new(3.15f64),
                Double::default()
            )
        ) => "table([3.14F,3.15F,00F] as `a)",
        table_build!(
            String::from("a") => Vector::<Double>::new()
        ) => "table([]$DOUBLE as `a)"
    );
    // string
    macro_test_upload!(
        test_upload_table_string,
        table_build!(
            String::from("a") => vector_build!(
                DolphinString,
                DolphinString::new(String::from("abc!@#中文 123")),
                DolphinString::new(String::from("abc!@#中文 124")),
                DolphinString::default()
            )
        ) => "table([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"] as `a)",
        table_build!(
            String::from("a") => Vector::<DolphinString>::new()
        ) => "table([]$STRING as `a)"
    );
    // blob
    macro_test_upload!(
        test_upload_table_blob,
        table_build!(
            String::from("a") => vector_build!(
                Blob,
                Blob::new("abc!@#中文 123".as_bytes().to_vec()),
                Blob::new("abc!@#中文 124".as_bytes().to_vec()),
                Blob::default()
            )
        ) => "table(blob([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]) as `a)",
        table_build!(
            String::from("a") => Vector::<Blob>::new()
        ) => "table([]$BLOB as `a)"
    );
    // decimal32
    macro_test_upload!(
        test_upload_table_decimal32,
        table_build!(
            String::from("a") => vector_build!(
                Decimal32,
                Decimal32::from_raw(314i32,2).unwrap(),
                Decimal32::from_raw(315i32,2).unwrap(),
                Decimal32::default()
            )
        ) => "table(decimal32([\"3.14\",\"3.15\",NULL],2) as `a)",
        table_build!(
            String::from("a") => Vector::<Decimal32>::new()
        ) => "table([]$DECIMAL32(0) as `a)"
    );
    // decimal64
    macro_test_upload!(
        test_upload_table_decimal64,
        table_build!(
            String::from("a") => vector_build!(
                Decimal64,
                Decimal64::from_raw(314i64,2).unwrap(),
                Decimal64::from_raw(315i64,2).unwrap(),
                Decimal64::default()
            )
        ) => "table(decimal64([\"3.14\",\"3.15\",NULL],2) as `a)",
        table_build!(
            String::from("a") => Vector::<Decimal64>::new()
        ) => "table([]$DECIMAL64(0) as `a)"
    );
    // decimal128
    macro_test_upload!(
        test_upload_table_decimal128,
        table_build!(
            String::from("a") => vector_build!(
                Decimal128,
                Decimal128::from_raw(314i128,2).unwrap(),
                Decimal128::from_raw(315i128,2).unwrap(),
                Decimal128::default()
            )
        ) => "table(decimal128([\"3.14\",\"3.15\",NULL],2) as `a)",
        table_build!(
            String::from("a") => Vector::<Decimal128>::new()
        ) => "table([]$DECIMAL128(0) as `a)"
    );
    // any
    macro_test_upload!(
        test_upload_table_any,
        table_build!(
            String::from("a") => vector_build!(
                Any,
                Any::new(Int::new(1i32).into()),
                Any::new(Int::new(2i32).into()),
                Any::new(Int::new(3i32).into())
            )
        ) => "table((1,2,3) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Any,
                Any::new(Int::new(1i32).into()),
                Any::new(vector_build!(Int,Int::new(1i32)).into()),
                Any::new(SetImpl::Int(set_build!(Int,Int::new(1i32))).into()),
                Any::new(DictionaryImpl::Int(dictionary_build!(Int,Int::new(1i32) => Int::new(1i32))).into()),
                Any::new(table_build!(
                    String::from("a") => vector_build!(
                        Int,
                        Int::new(1i32)
                    )
                ).into())
            )
        ) => "table((1,[1],set([1]),dict([1],[1]),table([1] as `a)) as `a)",
        table_build!(
            String::from("a") => Vector::<Any>::new()
        ) => "table([]$ANY as `a)"
    );
}
