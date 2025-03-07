mod setup;
mod utils;

use core::f32;
use dolphindb::client::ClientBuilder;
use dolphindb::types::*;
use encoding::{all::GBK, EncoderTrap, Encoding};

use setup::settings::Config;

macro_rules! macro_test_run_script {
    ($name:ident, $script:expr, $expect_form:expr, $expect_type:expr, $expect_value:expr, $path_constant_impl:path, $path:path) => {
        #[tokio::test]
        async fn $name() {
            // connect
            let conf = Config::new();
            let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
            builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
            let mut client = builder.connect().await.unwrap();
            // run_script
            let res = client.run_script($script).await.unwrap();
            // assert
            assert!(res.is_some());
            let res_constantimpl = res.unwrap();
            println!("{}", res_constantimpl);
            assert_eq!(res_constantimpl.data_form(), $expect_form);
            assert_eq!(res_constantimpl.data_type(), $expect_type);
            if let $path_constant_impl($path(res_)) = res_constantimpl {
                assert_eq!(res_, $expect_value);
            } else {
                assert!(false, "error in constant");
            }
        }
    };
}

mod test_run_script_scalar {
    use super::*;

    // void
    macro_test_run_script!(
        test_run_script_scalar_void_null,
        "NULL",
        DataForm::Scalar,
        DataType::Void,
        Void::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Void
    );
    macro_test_run_script!(
        test_run_script_scalar_void_dflt,
        "DFLT",
        DataForm::Scalar,
        DataType::Void,
        Void::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Void
    );
    // bool
    macro_test_run_script!(
        test_run_script_scalar_bool_true,
        "true",
        DataForm::Scalar,
        DataType::Bool,
        Bool::new(true),
        ConstantImpl::Scalar,
        ScalarImpl::Bool
    );
    macro_test_run_script!(
        test_run_script_scalar_bool_false,
        "false",
        DataForm::Scalar,
        DataType::Bool,
        Bool::new(false),
        ConstantImpl::Scalar,
        ScalarImpl::Bool
    );
    macro_test_run_script!(
        test_run_script_scalar_bool_null,
        "00b",
        DataForm::Scalar,
        DataType::Bool,
        Bool::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Bool
    );
    // char
    macro_test_run_script!(
        test_run_script_scalar_char_0,
        "0c",
        DataForm::Scalar,
        DataType::Char,
        Char::new(0i8),
        ConstantImpl::Scalar,
        ScalarImpl::Char
    );
    macro_test_run_script!(
        test_run_script_scalar_char_max,
        "127c",
        DataForm::Scalar,
        DataType::Char,
        Char::new(127i8),
        ConstantImpl::Scalar,
        ScalarImpl::Char
    );
    macro_test_run_script!(
        test_run_script_scalar_char_min,
        "-127c",
        DataForm::Scalar,
        DataType::Char,
        Char::new(-127i8),
        ConstantImpl::Scalar,
        ScalarImpl::Char
    );
    macro_test_run_script!(
        test_run_script_scalar_char_null,
        "00c",
        DataForm::Scalar,
        DataType::Char,
        Char::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Char
    );
    // short
    macro_test_run_script!(
        test_run_script_scalar_short_0,
        "0h",
        DataForm::Scalar,
        DataType::Short,
        Short::new(0i16),
        ConstantImpl::Scalar,
        ScalarImpl::Short
    );
    macro_test_run_script!(
        test_run_script_scalar_short_max,
        "32767h",
        DataForm::Scalar,
        DataType::Short,
        Short::new(32767i16),
        ConstantImpl::Scalar,
        ScalarImpl::Short
    );
    macro_test_run_script!(
        test_run_script_scalar_short_min,
        "-32767h",
        DataForm::Scalar,
        DataType::Short,
        Short::new(-32767i16),
        ConstantImpl::Scalar,
        ScalarImpl::Short
    );
    macro_test_run_script!(
        test_run_script_scalar_short_null,
        "00h",
        DataForm::Scalar,
        DataType::Short,
        Short::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Short
    );
    // int
    macro_test_run_script!(
        test_run_script_scalar_int_0,
        "0i",
        DataForm::Scalar,
        DataType::Int,
        Int::new(0i32),
        ConstantImpl::Scalar,
        ScalarImpl::Int
    );
    macro_test_run_script!(
        test_run_script_scalar_int_max,
        "2147483647i",
        DataForm::Scalar,
        DataType::Int,
        Int::new(2147483647i32),
        ConstantImpl::Scalar,
        ScalarImpl::Int
    );
    macro_test_run_script!(
        test_run_script_scalar_int_min,
        "-2147483647i",
        DataForm::Scalar,
        DataType::Int,
        Int::new(-2147483647i32),
        ConstantImpl::Scalar,
        ScalarImpl::Int
    );
    macro_test_run_script!(
        test_run_script_scalar_int_null,
        "00i",
        DataForm::Scalar,
        DataType::Int,
        Int::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Int
    );
    // long
    macro_test_run_script!(
        test_run_script_scalar_long_0,
        "0l",
        DataForm::Scalar,
        DataType::Long,
        Long::new(0i64),
        ConstantImpl::Scalar,
        ScalarImpl::Long
    );
    macro_test_run_script!(
        test_run_script_scalar_long_max,
        "9223372036854775807l",
        DataForm::Scalar,
        DataType::Long,
        Long::new(9223372036854775807i64),
        ConstantImpl::Scalar,
        ScalarImpl::Long
    );
    macro_test_run_script!(
        test_run_script_scalar_long_min,
        "-9223372036854775807l",
        DataForm::Scalar,
        DataType::Long,
        Long::new(-9223372036854775807i64),
        ConstantImpl::Scalar,
        ScalarImpl::Long
    );
    macro_test_run_script!(
        test_run_script_scalar_long_null,
        "00l",
        DataForm::Scalar,
        DataType::Long,
        Long::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Long
    );
    // date
    macro_test_run_script!(
        test_run_script_scalar_date_0,
        "1970.01.01d",
        DataForm::Scalar,
        DataType::Date,
        Date::from_ymd(1970, 1, 1).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Date
    );
    macro_test_run_script!(
        test_run_script_scalar_date_other,
        "2022.05.20d",
        DataForm::Scalar,
        DataType::Date,
        Date::from_ymd(2022, 5, 20).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Date
    );
    macro_test_run_script!(
        test_run_script_scalar_date_null,
        "00d",
        DataForm::Scalar,
        DataType::Date,
        Date::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Date
    );
    // month
    macro_test_run_script!(
        test_run_script_scalar_month_0,
        "month(0)",
        DataForm::Scalar,
        DataType::Month,
        Month::from_ym(0, 1).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Month
    );
    macro_test_run_script!(
        test_run_script_scalar_month_other,
        "1970.01M",
        DataForm::Scalar,
        DataType::Month,
        Month::from_ym(1970, 1).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Month
    );
    macro_test_run_script!(
        test_run_script_scalar_month_null,
        "00M",
        DataForm::Scalar,
        DataType::Month,
        Month::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Month
    );
    // time
    macro_test_run_script!(
        test_run_script_scalar_time_0,
        "00:00:00.000t",
        DataForm::Scalar,
        DataType::Time,
        Time::from_hms_milli(0, 0, 0, 0).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Time
    );
    macro_test_run_script!(
        test_run_script_scalar_time_other,
        "13:50:59.123t",
        DataForm::Scalar,
        DataType::Time,
        Time::from_hms_milli(13, 50, 59, 123).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Time
    );
    macro_test_run_script!(
        test_run_script_scalar_time_null,
        "00t",
        DataForm::Scalar,
        DataType::Time,
        Time::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Time
    );
    // minute
    macro_test_run_script!(
        test_run_script_scalar_minute_0,
        "00:00m",
        DataForm::Scalar,
        DataType::Minute,
        Minute::from_hm(0, 0).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_scalar_minute_other,
        "13:50m",
        DataForm::Scalar,
        DataType::Minute,
        Minute::from_hm(13, 50).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_scalar_minute_null,
        "00m",
        DataForm::Scalar,
        DataType::Minute,
        Minute::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Minute
    );
    // second
    macro_test_run_script!(
        test_run_script_scalar_second_0,
        "00:00:00s",
        DataForm::Scalar,
        DataType::Second,
        Second::from_hms(0, 0, 0).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Second
    );
    macro_test_run_script!(
        test_run_script_scalar_second_other,
        "13:50:59s",
        DataForm::Scalar,
        DataType::Second,
        Second::from_hms(13, 50, 59).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Second
    );
    macro_test_run_script!(
        test_run_script_scalar_second_null,
        "00s",
        DataForm::Scalar,
        DataType::Second,
        Second::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Second
    );
    // datetime
    macro_test_run_script!(
        test_run_script_scalar_datetime_0,
        "1970.01.01T00:00:00D",
        DataForm::Scalar,
        DataType::DateTime,
        DateTime::from_raw(0i32).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_scalar_datetime_other,
        "2022.05.20T13:50:59D",
        DataForm::Scalar,
        DataType::DateTime,
        DateTime::from_date_second(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Second::from_hms(13, 50, 59).unwrap()
        )
        .unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_scalar_datetime_null,
        "00D",
        DataForm::Scalar,
        DataType::DateTime,
        DateTime::default(),
        ConstantImpl::Scalar,
        ScalarImpl::DateTime
    );
    // timestamp
    macro_test_run_script!(
        test_run_script_scalar_timestamp_0,
        "1970.01.01T00:00:00.000T",
        DataForm::Scalar,
        DataType::Timestamp,
        Timestamp::from_raw(0i64).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_scalar_timestamp_other,
        "2022.05.20T13:50:59.123T",
        DataForm::Scalar,
        DataType::Timestamp,
        Timestamp::from_date_time(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap()
        )
        .unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_scalar_timestamp_null,
        "00T",
        DataForm::Scalar,
        DataType::Timestamp,
        Timestamp::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Timestamp
    );
    // nanotime
    macro_test_run_script!(
        test_run_script_scalar_nanotime_0,
        "00:00:00.000000000n",
        DataForm::Scalar,
        DataType::NanoTime,
        NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_scalar_nanotime_other,
        "13:50:59.123456789n",
        DataForm::Scalar,
        DataType::NanoTime,
        NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_scalar_nanotime_null,
        "00n",
        DataForm::Scalar,
        DataType::NanoTime,
        NanoTime::default(),
        ConstantImpl::Scalar,
        ScalarImpl::NanoTime
    );
    // nanotimestamp
    macro_test_run_script!(
        test_run_script_scalar_nanotimestamp_0,
        "1970.01.01T00:00:00.000000000N",
        DataForm::Scalar,
        DataType::NanoTimestamp,
        NanoTimestamp::from_raw(0).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_scalar_nanotimestamp_other,
        "2022.05.20T13:50:59.123456789N",
        DataForm::Scalar,
        DataType::NanoTimestamp,
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(2022, 5, 20).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
        )
        .unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_scalar_nanotimestamp_null,
        "00N",
        DataForm::Scalar,
        DataType::NanoTimestamp,
        NanoTimestamp::default(),
        ConstantImpl::Scalar,
        ScalarImpl::NanoTimestamp
    );
    // datehour
    macro_test_run_script!(
        test_run_script_scalar_datehour_0,
        "datehour('1970.01.01T00')",
        DataForm::Scalar,
        DataType::DateHour,
        DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_scalar_datehour_other,
        "datehour('2022.05.20T13')",
        DataForm::Scalar,
        DataType::DateHour,
        DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_scalar_datehour_null,
        "datehour(NULL)",
        DataForm::Scalar,
        DataType::DateHour,
        DateHour::default(),
        ConstantImpl::Scalar,
        ScalarImpl::DateHour
    );
    // float
    macro_test_run_script!(
        test_run_script_scalar_float_0,
        "0.0f",
        DataForm::Scalar,
        DataType::Float,
        Float::new(0.0f32),
        ConstantImpl::Scalar,
        ScalarImpl::Float
    );
    macro_test_run_script!(
        test_run_script_scalar_float_other,
        "3.14f",
        DataForm::Scalar,
        DataType::Float,
        Float::new(3.14f32),
        ConstantImpl::Scalar,
        ScalarImpl::Float
    );
    macro_test_run_script!(
        test_run_script_scalar_float_nan,
        "float('nan')",
        DataForm::Scalar,
        DataType::Float,
        Float::new(f32::NAN),
        ConstantImpl::Scalar,
        ScalarImpl::Float
    );
    macro_test_run_script!(
        test_run_script_scalar_float_inf,
        "float('inf')",
        DataForm::Scalar,
        DataType::Float,
        Float::new(f32::INFINITY),
        ConstantImpl::Scalar,
        ScalarImpl::Float
    );
    macro_test_run_script!(
        test_run_script_scalar_float_null,
        "00f",
        DataForm::Scalar,
        DataType::Float,
        Float::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Float
    );
    // double
    macro_test_run_script!(
        test_run_script_scalar_double_0,
        "0.0F",
        DataForm::Scalar,
        DataType::Double,
        Double::new(0.0f64),
        ConstantImpl::Scalar,
        ScalarImpl::Double
    );
    macro_test_run_script!(
        test_run_script_scalar_double_other,
        "3.14F",
        DataForm::Scalar,
        DataType::Double,
        Double::new(3.14f64),
        ConstantImpl::Scalar,
        ScalarImpl::Double
    );
    macro_test_run_script!(
        test_run_script_scalar_double_null,
        "00F",
        DataForm::Scalar,
        DataType::Double,
        Double::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Double
    );
    // string
    macro_test_run_script!(
        test_run_script_scalar_string_other,
        "'abc!@#中文 123'",
        DataForm::Scalar,
        DataType::String,
        DolphinString::new(String::from("abc!@#中文 123")),
        ConstantImpl::Scalar,
        ScalarImpl::String
    );
    macro_test_run_script!(
        test_run_script_scalar_string_null,
        "\"\"",
        DataForm::Scalar,
        DataType::String,
        DolphinString::default(),
        ConstantImpl::Scalar,
        ScalarImpl::String
    );
    // blob
    macro_test_run_script!(
        test_run_script_scalar_blob_other,
        "blob(\"abc!@#中文 123\")",
        DataForm::Scalar,
        DataType::Blob,
        Blob::new("abc!@#中文 123".as_bytes().to_vec()),
        ConstantImpl::Scalar,
        ScalarImpl::Blob
    );
    macro_test_run_script!(
        test_run_script_scalar_blob_gbk,
        "blob(fromUTF8(\"abc!@#中文 123\",\"gbk\"))",
        DataForm::Scalar,
        DataType::Blob,
        Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()),
        ConstantImpl::Scalar,
        ScalarImpl::Blob
    );
    macro_test_run_script!(
        test_run_script_scalar_blob_null,
        "blob(\"\")",
        DataForm::Scalar,
        DataType::Blob,
        Blob::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Blob
    );
    // decimal32
    macro_test_run_script!(
        test_run_script_scalar_decimal32_0_3,
        "decimal32(\"0\",3)",
        DataForm::Scalar,
        DataType::Decimal32,
        Decimal32::from_raw(0i32, 3).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal32
    );
    macro_test_run_script!(
        test_run_script_scalar_decimal32_8,
        "decimal32(\"3.141592653589\",8)",
        DataForm::Scalar,
        DataType::Decimal32,
        Decimal32::from_raw(314159265i32, 8).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal32
    );
    macro_test_run_script!(
        test_run_script_scalar_decimal32_9,
        "decimal32(\"-0.14159265358\",9)",
        DataForm::Scalar,
        DataType::Decimal32,
        Decimal32::from_raw(-141592654i32, 9).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal32
    );
    // todo:null的scale???
    macro_test_run_script!(
        test_run_script_scalar_decimal32_null,
        "decimal32(\"nan\",9)",
        DataForm::Scalar,
        DataType::Decimal32,
        Decimal32::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal32
    );
    // decimal64
    macro_test_run_script!(
        test_run_script_scalar_decimal64_0_3,
        "decimal64(\"0\",3)",
        DataForm::Scalar,
        DataType::Decimal64,
        Decimal64::from_raw(0i64, 3).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal64
    );
    macro_test_run_script!(
        test_run_script_scalar_decimal64_17,
        "decimal64(\"3.14159265358979323846\",17)",
        DataForm::Scalar,
        DataType::Decimal64,
        Decimal64::from_raw(314159265358979324i64, 17).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal64
    );
    macro_test_run_script!(
        test_run_script_scalar_decimal64_18,
        "decimal64(\"-0.14159265358979323846\",18)",
        DataForm::Scalar,
        DataType::Decimal64,
        Decimal64::from_raw(-141592653589793238i64, 18).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal64
    );
    macro_test_run_script!(
        test_run_script_scalar_decimal64_null,
        "decimal64(\"nan\",0)",
        DataForm::Scalar,
        DataType::Decimal64,
        Decimal64::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal64
    );
    // decimal128
    macro_test_run_script!(
        test_run_script_scalar_decimal128_0_3,
        "decimal128(\"0\",3)",
        DataForm::Scalar,
        DataType::Decimal128,
        Decimal128::from_raw(0i128, 3).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal128
    );
    macro_test_run_script!(
        test_run_script_scalar_decimal128_28,
        "decimal128(\"3.14159265358979323846264338327950288419\",28)",
        DataForm::Scalar,
        DataType::Decimal128,
        Decimal128::from_raw(31415926535897932384626433833i128, 28).unwrap(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal128
    );
    // only support 28
    // macro_test_run_script!(
    //     test_run_script_scalar_decimal128_37,
    //     "decimal128(\"3.14159265358979323846264338327950288419\",37)",
    //     DataForm::Scalar,
    //     DataType::Decimal128,
    //     Decimal128::from_raw(31415926535897932384626433832795028842i128, 37).unwrap(),
    //     ConstantImpl::Scalar,
    //     ScalarImpl::Decimal128
    // );
    // macro_test_run_script!(
    //     test_run_script_scalar_decimal128_38,
    //     "decimal128(\"-0.14159265358979323846264338327950288419\",38)",
    //     DataForm::Scalar,
    //     DataType::Decimal128,
    //     Decimal128::from_raw(-14159265358979323846264338327950288419i128, 38).unwrap(),
    //     ConstantImpl::Scalar,
    //     ScalarImpl::Decimal128
    // );
    macro_test_run_script!(
        test_run_script_scalar_decimal128_null,
        "decimal128(\"nan\",0)",
        DataForm::Scalar,
        DataType::Decimal128,
        Decimal128::default(),
        ConstantImpl::Scalar,
        ScalarImpl::Decimal128
    );
}

mod test_run_script_pair {
    use super::*;

    // bool
    macro_test_run_script!(
        test_run_script_pair_bool_normal,
        "pair(true,false)",
        DataForm::Pair,
        DataType::Bool,
        Pair::<Bool>::new((Bool::new(true), Bool::new(false))),
        ConstantImpl::Pair,
        PairImpl::Bool
    );
    macro_test_run_script!(
        test_run_script_pair_bool_contain_null,
        "pair(true,00b)",
        DataForm::Pair,
        DataType::Bool,
        Pair::<Bool>::new((Bool::new(true), Bool::default())),
        ConstantImpl::Pair,
        PairImpl::Bool
    );
    // cahr
    macro_test_run_script!(
        test_run_script_pair_char_normal,
        "pair(127c,-127c)",
        DataForm::Pair,
        DataType::Char,
        Pair::<Char>::new((Char::new(127i8), Char::new(-127i8))),
        ConstantImpl::Pair,
        PairImpl::Char
    );
    macro_test_run_script!(
        test_run_script_pair_char_contain_null,
        "pair(127c,00c)",
        DataForm::Pair,
        DataType::Char,
        Pair::<Char>::new((Char::new(127i8), Char::default())),
        ConstantImpl::Pair,
        PairImpl::Char
    );
    // short
    macro_test_run_script!(
        test_run_script_pair_short_normal,
        "pair(32767h,-32767h)",
        DataForm::Pair,
        DataType::Short,
        Pair::<Short>::new((Short::new(32767i16), Short::new(-32767i16))),
        ConstantImpl::Pair,
        PairImpl::Short
    );
    macro_test_run_script!(
        test_run_script_pair_short_contain_null,
        "pair(32767h,00h)",
        DataForm::Pair,
        DataType::Short,
        Pair::<Short>::new((Short::new(32767i16), Short::default())),
        ConstantImpl::Pair,
        PairImpl::Short
    );
    // int
    macro_test_run_script!(
        test_run_script_pair_int_normal,
        "pair(2147483647i,-2147483647i)",
        DataForm::Pair,
        DataType::Int,
        Pair::<Int>::new((Int::new(2147483647i32), Int::new(-2147483647i32))),
        ConstantImpl::Pair,
        PairImpl::Int
    );
    macro_test_run_script!(
        test_run_script_pair_int_contain_null,
        "pair(2147483647i,00i)",
        DataForm::Pair,
        DataType::Int,
        Pair::<Int>::new((Int::new(2147483647i32), Int::default())),
        ConstantImpl::Pair,
        PairImpl::Int
    );
    // long
    macro_test_run_script!(
        test_run_script_pair_long_normal,
        "pair(9223372036854775807l,-9223372036854775807l)",
        DataForm::Pair,
        DataType::Long,
        Pair::<Long>::new((
            Long::new(9223372036854775807i64),
            Long::new(-9223372036854775807i64)
        )),
        ConstantImpl::Pair,
        PairImpl::Long
    );
    macro_test_run_script!(
        test_run_script_pair_long_contain_null,
        "pair(9223372036854775807l,00l)",
        DataForm::Pair,
        DataType::Long,
        Pair::<Long>::new((Long::new(9223372036854775807i64), Long::default())),
        ConstantImpl::Pair,
        PairImpl::Long
    );
    // date
    macro_test_run_script!(
        test_run_script_pair_date_normal,
        "pair(1970.01.01d,2022.05.20d)",
        DataForm::Pair,
        DataType::Date,
        Pair::<Date>::new((
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(2022, 5, 20).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Date
    );
    macro_test_run_script!(
        test_run_script_pair_date_contain_null,
        "pair(1970.01.01d,00d)",
        DataForm::Pair,
        DataType::Date,
        Pair::<Date>::new((Date::from_ymd(1970, 1, 1).unwrap(), Date::default())),
        ConstantImpl::Pair,
        PairImpl::Date
    );
    // month
    macro_test_run_script!(
        test_run_script_pair_month_normal,
        "pair(1970.01M,2022.05M)",
        DataForm::Pair,
        DataType::Month,
        Pair::<Month>::new((
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(2022, 5).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Month
    );
    macro_test_run_script!(
        test_run_script_pair_month_contain_null,
        "pair(1970.01M,00M)",
        DataForm::Pair,
        DataType::Month,
        Pair::<Month>::new((Month::from_ym(1970, 1).unwrap(), Month::default())),
        ConstantImpl::Pair,
        PairImpl::Month
    );
    // time
    macro_test_run_script!(
        test_run_script_pair_time_normal,
        "pair(00:00:00.000t,13:50:59.123t)",
        DataForm::Pair,
        DataType::Time,
        Pair::<Time>::new((
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Time
    );
    macro_test_run_script!(
        test_run_script_pair_time_contain_null,
        "pair(00:00:00.000t,00t)",
        DataForm::Pair,
        DataType::Time,
        Pair::<Time>::new((Time::from_hms_milli(0, 0, 0, 0).unwrap(), Time::default())),
        ConstantImpl::Pair,
        PairImpl::Time
    );
    // minute
    macro_test_run_script!(
        test_run_script_pair_minute_normal,
        "pair(00:00m,13:50m)",
        DataForm::Pair,
        DataType::Minute,
        Pair::<Minute>::new((
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(13, 50).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_pair_minute_contain_null,
        "pair(00:00m,00m)",
        DataForm::Pair,
        DataType::Minute,
        Pair::<Minute>::new((Minute::from_hm(0, 0).unwrap(), Minute::default())),
        ConstantImpl::Pair,
        PairImpl::Minute
    );
    // second
    macro_test_run_script!(
        test_run_script_pair_second_normal,
        "pair(00:00:00s,13:50:59s)",
        DataForm::Pair,
        DataType::Second,
        Pair::<Second>::new((
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(13, 50, 59).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Second
    );
    macro_test_run_script!(
        test_run_script_pair_second_contain_null,
        "pair(00:00:00s,00s)",
        DataForm::Pair,
        DataType::Second,
        Pair::<Second>::new((Second::from_hms(0, 0, 0).unwrap(), Second::default())),
        ConstantImpl::Pair,
        PairImpl::Second
    );
    // datetime
    macro_test_run_script!(
        test_run_script_pair_datetime_normal,
        "pair(1970.01.01T00:00:00D,2022.05.20T13:50:59D)",
        DataForm::Pair,
        DataType::DateTime,
        Pair::<DateTime>::new((
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            )
            .unwrap(),
            DateTime::from_date_second(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Second::from_hms(13, 50, 59).unwrap()
            )
            .unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_pair_datetime_contain_null,
        "pair(1970.01.01T00:00:00D,00D)",
        DataForm::Pair,
        DataType::DateTime,
        Pair::<DateTime>::new((
            DateTime::from_date_second(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Second::from_hms(0, 0, 0).unwrap()
            )
            .unwrap(),
            DateTime::default()
        )),
        ConstantImpl::Pair,
        PairImpl::DateTime
    );
    // timestamp
    macro_test_run_script!(
        test_run_script_pair_timestamp_normal,
        "pair(1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T)",
        DataForm::Pair,
        DataType::Timestamp,
        Pair::<Timestamp>::new((
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            )
            .unwrap(),
            Timestamp::from_date_time(
                Date::from_ymd(2022, 5, 20).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap()
            )
            .unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_pair_timestamp_contain_null,
        "pair(1970.01.01T00:00:00.000T,00T)",
        DataForm::Pair,
        DataType::Timestamp,
        Pair::<Timestamp>::new((
            Timestamp::from_date_time(
                Date::from_ymd(1970, 1, 1).unwrap(),
                Time::from_hms_milli(0, 0, 0, 0).unwrap()
            )
            .unwrap(),
            Timestamp::default()
        )),
        ConstantImpl::Pair,
        PairImpl::Timestamp
    );
    // nanotime
    macro_test_run_script!(
        test_run_script_pair_nanotime_normal,
        "pair(00:00:00.000000000n,13:50:59.123456789n)",
        DataForm::Pair,
        DataType::NanoTime,
        Pair::<NanoTime>::new((
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_pair_nanotime_contain_null,
        "pair(00:00:00.000000000n,00n)",
        DataForm::Pair,
        DataType::NanoTime,
        Pair::<NanoTime>::new((
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::default()
        )),
        ConstantImpl::Pair,
        PairImpl::NanoTime
    );
    // nanotimestamp
    macro_test_run_script!(
        test_run_script_pair_nanotimestamp_normal,
        "pair(1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N)",
        DataForm::Pair,
        DataType::NanoTimestamp,
        Pair::<NanoTimestamp>::new((
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            )
            .unwrap(),
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            )
            .unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_pair_nanotimestamp_contain_null,
        "pair(1970.01.01T00:00:00.000000000N,00N)",
        DataForm::Pair,
        DataType::NanoTimestamp,
        Pair::<NanoTimestamp>::new((
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            )
            .unwrap(),
            NanoTimestamp::default()
        )),
        ConstantImpl::Pair,
        PairImpl::NanoTimestamp
    );
    // datehour
    macro_test_run_script!(
        test_run_script_pair_datehour_normal,
        "pair(datehour('1970.01.01T00'),datehour('2022.05.20T13'))",
        DataForm::Pair,
        DataType::DateHour,
        Pair::<DateHour>::new((
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_pair_datehour_contain_null,
        "pair(datehour('1970.01.01T00'),datehour(NULL))",
        DataForm::Pair,
        DataType::DateHour,
        Pair::<DateHour>::new((
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::default()
        )),
        ConstantImpl::Pair,
        PairImpl::DateHour
    );
    // float
    macro_test_run_script!(
        test_run_script_pair_float_normal,
        "pair(0.0f,3.14f)",
        DataForm::Pair,
        DataType::Float,
        Pair::<Float>::new((Float::new(0.0f32), Float::new(3.14f32))),
        ConstantImpl::Pair,
        PairImpl::Float
    );
    macro_test_run_script!(
        test_run_script_pair_float_contain_null,
        "pair(0.0f,00f)",
        DataForm::Pair,
        DataType::Float,
        Pair::<Float>::new((Float::new(0.0f32), Float::default())),
        ConstantImpl::Pair,
        PairImpl::Float
    );
    // double
    macro_test_run_script!(
        test_run_script_pair_double_normal,
        "pair(0.0F,3.14F)",
        DataForm::Pair,
        DataType::Double,
        Pair::<Double>::new((Double::new(0.0f64), Double::new(3.14f64))),
        ConstantImpl::Pair,
        PairImpl::Double
    );
    macro_test_run_script!(
        test_run_script_pair_double_contain_null,
        "pair(0.0F,00F)",
        DataForm::Pair,
        DataType::Double,
        Pair::<Double>::new((Double::new(0.0f64), Double::default())),
        ConstantImpl::Pair,
        PairImpl::Double
    );
    // symbol
    macro_test_run_script!(
        test_run_script_pair_symbol_normal,
        "symbol(pair('abc!@#中文 123','abc!@#中文 124'))",
        DataForm::Pair,
        DataType::Symbol,
        Pair::<Symbol>::new((
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::new(String::from("abc!@#中文 124"))
        )),
        ConstantImpl::Pair,
        PairImpl::Symbol
    );
    macro_test_run_script!(
        test_run_script_pair_symbol_contain_null,
        "symbol(pair('abc!@#中文 123',\"\"))",
        DataForm::Pair,
        DataType::Symbol,
        Pair::<Symbol>::new((
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::default()
        )),
        ConstantImpl::Pair,
        PairImpl::Symbol
    );
    // string
    macro_test_run_script!(
        test_run_script_pair_string_normal,
        "pair('abc!@#中文 123','abc!@#中文 124')",
        DataForm::Pair,
        DataType::String,
        Pair::<DolphinString>::new((
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("abc!@#中文 124"))
        )),
        ConstantImpl::Pair,
        PairImpl::String
    );
    macro_test_run_script!(
        test_run_script_pair_string_contain_null,
        "pair('abc!@#中文 123',\"\")",
        DataForm::Pair,
        DataType::String,
        Pair::<DolphinString>::new((
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::default()
        )),
        ConstantImpl::Pair,
        PairImpl::String
    );
    // blob
    // todo:RUS-21
    // macro_test_run_script!(
    //     test_run_script_pair_blob_normal,
    //     "pair(blob('abc!@#中文 123'),blob('abc!@#中文 124'))",
    //     DataForm::Pair,
    //     DataType::Blob,
    //     Pair::<Blob>::new((
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         Blob::new("abc!@#中文 124".as_bytes().to_vec())
    //     )),
    //     ConstantImpl::Pair,
    //     PairImpl::Blob
    // );
    // macro_test_run_script!(
    //     test_run_script_pair_blob_contain_null,
    //     "pair(blob('abc!@#中文 123'),blob(\"\"))",
    //     DataForm::Pair,
    //     DataType::Blob,
    //     Pair::<Blob>::new((
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         Blob::default()
    //     )),
    //     ConstantImpl::Pair,
    //     PairImpl::Blob
    // );
    // decimal32
    macro_test_run_script!(
        test_run_script_pair_decimal32_normal,
        "pair(decimal32('3.14',2),decimal32('3.15',2))",
        DataForm::Pair,
        DataType::Decimal32,
        Pair::<Decimal32>::new((
            Decimal32::from_raw(314, 2).unwrap(),
            Decimal32::from_raw(315, 2).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Decimal32
    );
    macro_test_run_script!(
        test_run_script_pair_decimal32_contain_null,
        "pair(decimal32('3.14',2),decimal32(NULL,2))",
        DataForm::Pair,
        DataType::Decimal32,
        Pair::<Decimal32>::new((Decimal32::from_raw(314, 2).unwrap(), Decimal32::default())),
        ConstantImpl::Pair,
        PairImpl::Decimal32
    );
    // decimal64
    macro_test_run_script!(
        test_run_script_pair_decimal64_normal,
        "pair(decimal64('3.14',2),decimal64('3.15',2))",
        DataForm::Pair,
        DataType::Decimal64,
        Pair::<Decimal64>::new((
            Decimal64::from_raw(314, 2).unwrap(),
            Decimal64::from_raw(315, 2).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Decimal64
    );
    macro_test_run_script!(
        test_run_script_pair_decimal64_contain_null,
        "pair(decimal64('3.14',2),decimal64(NULL,2))",
        DataForm::Pair,
        DataType::Decimal64,
        Pair::<Decimal64>::new((Decimal64::from_raw(314, 2).unwrap(), Decimal64::default())),
        ConstantImpl::Pair,
        PairImpl::Decimal64
    );
    // decimal128
    macro_test_run_script!(
        test_run_script_pair_decimal128_normal,
        "pair(decimal128('3.14',2),decimal128('3.15',2))",
        DataForm::Pair,
        DataType::Decimal128,
        Pair::<Decimal128>::new((
            Decimal128::from_raw(314, 2).unwrap(),
            Decimal128::from_raw(315, 2).unwrap()
        )),
        ConstantImpl::Pair,
        PairImpl::Decimal128
    );
    macro_test_run_script!(
        test_run_script_pair_decimal128_contain_null,
        "pair(decimal128('3.14',2),decimal128(NULL,2))",
        DataForm::Pair,
        DataType::Decimal128,
        Pair::<Decimal128>::new((Decimal128::from_raw(314, 2).unwrap(), Decimal128::default())),
        ConstantImpl::Pair,
        PairImpl::Decimal128
    );
}

mod test_run_script_vector {
    use super::*;

    // bool
    macro_test_run_script!(
        test_run_script_vector_bool_normal,
        "x=array(BOOL,0,3).append!([true,false,true]);x",
        DataForm::Vector,
        DataType::Bool,
        Vector::<Bool>::from_raw(&[true, false, true]),
        ConstantImpl::Vector,
        VectorImpl::Bool
    );
    macro_test_run_script!(
        test_run_script_vector_bool_empty,
        "array(BOOL)",
        DataForm::Vector,
        DataType::Bool,
        Vector::<Bool>::new(),
        ConstantImpl::Vector,
        VectorImpl::Bool
    );
    macro_test_run_script!(
        test_run_script_vector_bool_contain_none,
        "x=array(BOOL,0,3).append!([true,false,00b]);x",
        DataForm::Vector,
        DataType::Bool,
        vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default()),
        ConstantImpl::Vector,
        VectorImpl::Bool
    );
    macro_test_run_script!(
        test_run_script_vector_bool_all_null,
        "x=array(BOOL,0,3).append!([00b,00b,00b]);x",
        DataForm::Vector,
        DataType::Bool,
        vector_build!(Bool, Bool::default(), Bool::default(), Bool::default()),
        ConstantImpl::Vector,
        VectorImpl::Bool
    );
    // char
    macro_test_run_script!(
        test_run_script_vector_char_normal,
        "x=array(CHAR,0,3).append!([0c,127c,-127c]);x",
        DataForm::Vector,
        DataType::Char,
        Vector::<Char>::from_raw(&[0i8, 127i8, -127i8]),
        ConstantImpl::Vector,
        VectorImpl::Char
    );
    macro_test_run_script!(
        test_run_script_vector_char_empty,
        "array(CHAR)",
        DataForm::Vector,
        DataType::Char,
        Vector::<Char>::new(),
        ConstantImpl::Vector,
        VectorImpl::Char
    );
    macro_test_run_script!(
        test_run_script_vector_char_contain_none,
        "x=array(CHAR,0,4).append!([0c,127c,-127c,00c]);x",
        DataForm::Vector,
        DataType::Char,
        vector_build!(
            Char,
            Char::new(0i8),
            Char::new(127i8),
            Char::new(-127i8),
            Char::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Char
    );
    macro_test_run_script!(
        test_run_script_vector_char_all_null,
        "x=array(CHAR,0,3).append!([00c,00c,00c]);x",
        DataForm::Vector,
        DataType::Char,
        vector_build!(Char, Char::default(), Char::default(), Char::default()),
        ConstantImpl::Vector,
        VectorImpl::Char
    );
    // short
    macro_test_run_script!(
        test_run_script_vector_short_normal,
        "x=array(SHORT,0,3).append!([0h,32767h,-32767h]);x",
        DataForm::Vector,
        DataType::Short,
        Vector::<Short>::from_raw(&[0i16, 32767i16, -32767i16]),
        ConstantImpl::Vector,
        VectorImpl::Short
    );
    macro_test_run_script!(
        test_run_script_vector_short_empty,
        "array(SHORT)",
        DataForm::Vector,
        DataType::Short,
        Vector::<Short>::new(),
        ConstantImpl::Vector,
        VectorImpl::Short
    );
    macro_test_run_script!(
        test_run_script_vector_short_contain_none,
        "x=array(SHORT,0,4).append!([0h,32767h,-32767h,00h]);x",
        DataForm::Vector,
        DataType::Short,
        vector_build!(
            Short,
            Short::new(0i16),
            Short::new(32767i16),
            Short::new(-32767i16),
            Short::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Short
    );
    macro_test_run_script!(
        test_run_script_vector_short_all_null,
        "x=array(SHORT,0,3).append!([00h,00h,00h]);x",
        DataForm::Vector,
        DataType::Short,
        vector_build!(Short, Short::default(), Short::default(), Short::default()),
        ConstantImpl::Vector,
        VectorImpl::Short
    );
    // int
    macro_test_run_script!(
        test_run_script_vector_int_normal,
        "x=array(INT,0,3).append!([0i,2147483647i,-2147483647i]);x",
        DataForm::Vector,
        DataType::Int,
        Vector::<Int>::from_raw(&[0i32, 2147483647i32, -2147483647i32]),
        ConstantImpl::Vector,
        VectorImpl::Int
    );
    macro_test_run_script!(
        test_run_script_vector_int_empty,
        "array(INT)",
        DataForm::Vector,
        DataType::Int,
        Vector::<Int>::new(),
        ConstantImpl::Vector,
        VectorImpl::Int
    );
    macro_test_run_script!(
        test_run_script_vector_int_contain_none,
        "x=array(INT,0,4).append!([0i,2147483647i,-2147483647i,00i]);x",
        DataForm::Vector,
        DataType::Int,
        vector_build!(
            Int,
            Int::new(0i32),
            Int::new(2147483647i32),
            Int::new(-2147483647i32),
            Int::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Int
    );
    macro_test_run_script!(
        test_run_script_vector_int_all_null,
        "x=array(INT,0,3).append!([00i,00i,00i]);x",
        DataForm::Vector,
        DataType::Int,
        vector_build!(Int, Int::default(), Int::default(), Int::default()),
        ConstantImpl::Vector,
        VectorImpl::Int
    );
    // long
    macro_test_run_script!(
        test_run_script_vector_long_normal,
        "x=array(LONG,0,3).append!([0l,9223372036854775807l,-9223372036854775807l]);x",
        DataForm::Vector,
        DataType::Long,
        Vector::<Long>::from_raw(&[0i64, 9223372036854775807i64, -9223372036854775807i64]),
        ConstantImpl::Vector,
        VectorImpl::Long
    );
    macro_test_run_script!(
        test_run_script_vector_long_empty,
        "array(LONG)",
        DataForm::Vector,
        DataType::Long,
        Vector::<Long>::new(),
        ConstantImpl::Vector,
        VectorImpl::Long
    );
    macro_test_run_script!(
        test_run_script_vector_long_contain_none,
        "x=array(LONG,0,4).append!([0l,9223372036854775807l,-9223372036854775807l,00l]);x",
        DataForm::Vector,
        DataType::Long,
        vector_build!(
            Long,
            Long::new(0i64),
            Long::new(9223372036854775807i64),
            Long::new(-9223372036854775807i64),
            Long::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Long
    );
    macro_test_run_script!(
        test_run_script_vector_long_all_null,
        "x=array(LONG,0,3).append!([00l,00l,00l]);x",
        DataForm::Vector,
        DataType::Long,
        vector_build!(Long, Long::default(), Long::default(), Long::default()),
        ConstantImpl::Vector,
        VectorImpl::Long
    );
    // date
    macro_test_run_script!(
        test_run_script_vector_date_normal,
        "x=array(DATE,0,2).append!([1970.01.01d,2022.05.20d]);x",
        DataForm::Vector,
        DataType::Date,
        vector_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(2022, 5, 20).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::Date
    );
    macro_test_run_script!(
        test_run_script_vector_date_empty,
        "array(DATE)",
        DataForm::Vector,
        DataType::Date,
        Vector::<Date>::new(),
        ConstantImpl::Vector,
        VectorImpl::Date
    );
    macro_test_run_script!(
        test_run_script_vector_date_contain_none,
        "x=array(DATE,0,3).append!([1970.01.01d,2022.05.20d,00d]);x",
        DataForm::Vector,
        DataType::Date,
        vector_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(2022, 5, 20).unwrap(),
            Date::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Date
    );
    macro_test_run_script!(
        test_run_script_vector_date_all_null,
        "x=array(DATE,0,3).append!([00d,00d,00d]);x",
        DataForm::Vector,
        DataType::Date,
        vector_build!(Date, Date::default(), Date::default(), Date::default()),
        ConstantImpl::Vector,
        VectorImpl::Date
    );
    // month
    macro_test_run_script!(
        test_run_script_vector_month_normal,
        "x=array(MONTH,0,2).append!([1970.01M,2022.05M]);x",
        DataForm::Vector,
        DataType::Month,
        vector_build!(
            Month,
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(2022, 5).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::Month
    );
    macro_test_run_script!(
        test_run_script_vector_month_empty,
        "array(MONTH)",
        DataForm::Vector,
        DataType::Month,
        Vector::<Month>::new(),
        ConstantImpl::Vector,
        VectorImpl::Month
    );
    macro_test_run_script!(
        test_run_script_vector_month_contain_none,
        "x=array(MONTH,0,3).append!([1970.01M,2022.05M,00M]);x",
        DataForm::Vector,
        DataType::Month,
        vector_build!(
            Month,
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(2022, 5).unwrap(),
            Month::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Month
    );
    macro_test_run_script!(
        test_run_script_vector_month_all_null,
        "x=array(MONTH,0,3).append!([00d,00d,00d]);x",
        DataForm::Vector,
        DataType::Month,
        vector_build!(Month, Month::default(), Month::default(), Month::default()),
        ConstantImpl::Vector,
        VectorImpl::Month
    );
    // time
    macro_test_run_script!(
        test_run_script_vector_time_normal,
        "x=array(TIME,0,2).append!([00:00:00.000t,13:50:59.123t]);x",
        DataForm::Vector,
        DataType::Time,
        vector_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::Time
    );
    macro_test_run_script!(
        test_run_script_vector_time_empty,
        "array(TIME)",
        DataForm::Vector,
        DataType::Time,
        Vector::<Time>::new(),
        ConstantImpl::Vector,
        VectorImpl::Time
    );
    macro_test_run_script!(
        test_run_script_vector_time_contain_none,
        "x=array(TIME,0,3).append!([00:00:00.000t,13:50:59.123t,00t]);x",
        DataForm::Vector,
        DataType::Time,
        vector_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            Time::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Time
    );
    macro_test_run_script!(
        test_run_script_vector_time_all_null,
        "x=array(TIME,0,3).append!([00t,00t,00t]);x",
        DataForm::Vector,
        DataType::Time,
        vector_build!(Time, Time::default(), Time::default(), Time::default()),
        ConstantImpl::Vector,
        VectorImpl::Time
    );
    // minute
    macro_test_run_script!(
        test_run_script_vector_minute_normal,
        "x=array(MINUTE,0,2).append!([00:00m,13:50m]);x",
        DataForm::Vector,
        DataType::Minute,
        vector_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(13, 50).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_vector_minute_empty,
        "array(MINUTE)",
        DataForm::Vector,
        DataType::Minute,
        Vector::<Minute>::new(),
        ConstantImpl::Vector,
        VectorImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_vector_minute_contain_none,
        "x=array(MINUTE,0,3).append!([00:00m,13:50m,00m]);x",
        DataForm::Vector,
        DataType::Minute,
        vector_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(13, 50).unwrap(),
            Minute::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_vector_minute_all_null,
        "x=array(MINUTE,0,3).append!([00m,00m,00m]);x",
        DataForm::Vector,
        DataType::Minute,
        vector_build!(
            Minute,
            Minute::default(),
            Minute::default(),
            Minute::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Minute
    );
    // second
    macro_test_run_script!(
        test_run_script_vector_second_normal,
        "x=array(SECOND,0,2).append!([00:00:00s,13:50:59s]);x",
        DataForm::Vector,
        DataType::Second,
        vector_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(13, 50, 59).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::Second
    );
    macro_test_run_script!(
        test_run_script_vector_second_empty,
        "array(SECOND)",
        DataForm::Vector,
        DataType::Second,
        Vector::<Second>::new(),
        ConstantImpl::Vector,
        VectorImpl::Second
    );
    macro_test_run_script!(
        test_run_script_vector_second_contain_none,
        "x=array(SECOND,0,3).append!([00:00:00s,13:50:59s,00s]);x",
        DataForm::Vector,
        DataType::Second,
        vector_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(13, 50, 59).unwrap(),
            Second::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Second
    );
    macro_test_run_script!(
        test_run_script_vector_second_all_null,
        "x=array(SECOND,0,3).append!([00s,00s,00s]);x",
        DataForm::Vector,
        DataType::Second,
        vector_build!(
            Second,
            Second::default(),
            Second::default(),
            Second::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Second
    );
    // datetime
    macro_test_run_script!(
        test_run_script_vector_datetime_normal,
        "x=array(DATETIME,0,2).append!([1970.01.01T00:00:00D,2022.05.20T13:50:59D]);x",
        DataForm::Vector,
        DataType::DateTime,
        vector_build!(
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
            .unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_vector_datetime_empty,
        "array(DATETIME)",
        DataForm::Vector,
        DataType::DateTime,
        Vector::<DateTime>::new(),
        ConstantImpl::Vector,
        VectorImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_vector_datetime_contain_none,
        "x=array(DATETIME,0,3).append!([1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D]);x",
        DataForm::Vector,
        DataType::DateTime,
        vector_build!(
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
        ),
        ConstantImpl::Vector,
        VectorImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_vector_datetime_all_null,
        "x=array(DATETIME,0,3).append!([00D,00D,00D]);x",
        DataForm::Vector,
        DataType::DateTime,
        vector_build!(
            DateTime,
            DateTime::default(),
            DateTime::default(),
            DateTime::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::DateTime
    );
    // timestamp
    macro_test_run_script!(
        test_run_script_vector_timestamp_normal,
        "x=array(TIMESTAMP,0,2).append!([1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T]);x",
        DataForm::Vector,
        DataType::Timestamp,
        vector_build!(
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
            .unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_vector_timestamp_empty,
        "array(TIMESTAMP)",
        DataForm::Vector,
        DataType::Timestamp,
        Vector::<Timestamp>::new(),
        ConstantImpl::Vector,
        VectorImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_vector_timestamp_contain_none,
        "x=array(TIMESTAMP,0,3).append!([1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T]);x",
        DataForm::Vector,
        DataType::Timestamp,
        vector_build!(
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
        ),
        ConstantImpl::Vector,
        VectorImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_vector_timestamp_all_null,
        "x=array(TIMESTAMP,0,3).append!([00T,00T,00T]);x",
        DataForm::Vector,
        DataType::Timestamp,
        vector_build!(
            Timestamp,
            Timestamp::default(),
            Timestamp::default(),
            Timestamp::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Timestamp
    );
    // nanotime
    macro_test_run_script!(
        test_run_script_vector_nanotime_normal,
        "x=array(NANOTIME,0,2).append!([00:00:00.000000000n,13:50:59.123456789n]);x",
        DataForm::Vector,
        DataType::NanoTime,
        vector_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_vector_nanotime_empty,
        "array(NANOTIME)",
        DataForm::Vector,
        DataType::NanoTime,
        Vector::<NanoTime>::new(),
        ConstantImpl::Vector,
        VectorImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_vector_nanotime_contain_none,
        "x=array(NANOTIME,0,3).append!([00:00:00.000000000n,13:50:59.123456789n,00n]);x",
        DataForm::Vector,
        DataType::NanoTime,
        vector_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            NanoTime::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_vector_nanotime_all_null,
        "x=array(NANOTIME,0,3).append!([00n,00n,00n]);x",
        DataForm::Vector,
        DataType::NanoTime,
        vector_build!(
            NanoTime,
            NanoTime::default(),
            NanoTime::default(),
            NanoTime::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::NanoTime
    );
    // nanotimestamp
    macro_test_run_script!(
        test_run_script_vector_nanotimestamp_normal,
        "x=array(NANOTIMESTAMP,0,2).append!([1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N]);x",
        DataForm::Vector,
        DataType::NanoTimestamp,
        vector_build!(
            NanoTimestamp,
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(1970, 1, 1).unwrap(),
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
            ).unwrap(),
            NanoTimestamp::from_date_nanotime(
                Date::from_ymd(2022, 5, 20).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
            ).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_vector_nanotimestamp_empty,
        "array(NANOTIMESTAMP)",
        DataForm::Vector,
        DataType::NanoTimestamp,
        Vector::<NanoTimestamp>::new(),
        ConstantImpl::Vector,
        VectorImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_vector_nanotimestamp_contain_none,
        "x=array(NANOTIMESTAMP,0,3).append!([1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N]);x",
        DataForm::Vector,
        DataType::NanoTimestamp,
        vector_build!(
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
        ),
        ConstantImpl::Vector,
        VectorImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_vector_nanotimestamp_all_null,
        "x=array(NANOTIMESTAMP,0,3).append!([00N,00N,00N]);x",
        DataForm::Vector,
        DataType::NanoTimestamp,
        vector_build!(
            NanoTimestamp,
            NanoTimestamp::default(),
            NanoTimestamp::default(),
            NanoTimestamp::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::NanoTimestamp
    );
    // datehour
    macro_test_run_script!(
        test_run_script_vector_datehour_normal,
        "x=array(DATEHOUR,0,2).append!(datehour([\"1970.01.01T00\",\"2022.05.20T13\"]));x",
        DataForm::Vector,
        DataType::DateHour,
        vector_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap()
        ),
        ConstantImpl::Vector,
        VectorImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_vector_datehour_empty,
        "array(DATEHOUR)",
        DataForm::Vector,
        DataType::DateHour,
        Vector::<DateHour>::new(),
        ConstantImpl::Vector,
        VectorImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_vector_datehour_contain_none,
        "x=array(DATEHOUR,0,3).append!(datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]));x",
        DataForm::Vector,
        DataType::DateHour,
        vector_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DateHour::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_vector_datehour_all_null,
        "x=array(DATEHOUR,0,3).append!([datehour(NULL),datehour(NULL),datehour(NULL)]);x",
        DataForm::Vector,
        DataType::DateHour,
        vector_build!(
            DateHour,
            DateHour::default(),
            DateHour::default(),
            DateHour::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::DateHour
    );
    // float
    macro_test_run_script!(
        test_run_script_vector_float_normal,
        "x=array(FLOAT,0,4).append!([0.0f,float(`nan),float(`inf),00f]);x",
        DataForm::Vector,
        DataType::Float,
        vector_build!(
            Float,
            Float::new(0.0f32),
            Float::new(f32::NAN),
            Float::new(f32::INFINITY),
            Float::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Float
    );
    macro_test_run_script!(
        test_run_script_vector_float_empty,
        "array(FLOAT)",
        DataForm::Vector,
        DataType::Float,
        Vector::<Float>::new(),
        ConstantImpl::Vector,
        VectorImpl::Float
    );
    macro_test_run_script!(
        test_run_script_vector_float_all_null,
        "x=array(FLOAT,0,3).append!([00f,00f,00f]);x",
        DataForm::Vector,
        DataType::Float,
        vector_build!(Float, Float::default(), Float::default(), Float::default()),
        ConstantImpl::Vector,
        VectorImpl::Float
    );
    // double
    macro_test_run_script!(
        test_run_script_vector_double_normal,
        "x=array(DOUBLE,0,3).append!([0.0F,3.14F,00F]);x",
        DataForm::Vector,
        DataType::Double,
        vector_build!(
            Double,
            Double::new(0.0f64),
            Double::new(3.14f64),
            Double::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Double
    );
    macro_test_run_script!(
        test_run_script_vector_double_empty,
        "array(DOUBLE)",
        DataForm::Vector,
        DataType::Double,
        Vector::<Double>::new(),
        ConstantImpl::Vector,
        VectorImpl::Double
    );
    macro_test_run_script!(
        test_run_script_vector_double_all_null,
        "x=array(DOUBLE,0,3).append!([00f,00f,00f]);x",
        DataForm::Vector,
        DataType::Double,
        vector_build!(
            Double,
            Double::default(),
            Double::default(),
            Double::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Double
    );
    // symbol
    macro_test_run_script!(
        test_run_script_vector_symbol_normal,
        "x=array(SYMBOL,0,2).append!(symbol([\"abc!@#中文 123\",\"\"]));x",
        DataForm::Vector,
        DataType::Symbol,
        vector_build!(
            Symbol,
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Symbol
    );
    macro_test_run_script!(
        test_run_script_vector_symbol_empty,
        "array(SYMBOL)",
        DataForm::Vector,
        DataType::Symbol,
        Vector::<Symbol>::new(),
        ConstantImpl::Vector,
        VectorImpl::Symbol
    );
    macro_test_run_script!(
        test_run_script_vector_symbol_all_null,
        "x=array(SYMBOL,0,3).append!(symbol([\"\",\"\",\"\"]));x",
        DataForm::Vector,
        DataType::Symbol,
        vector_build!(
            Symbol,
            Symbol::default(),
            Symbol::default(),
            Symbol::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Symbol
    );
    // string
    macro_test_run_script!(
        test_run_script_vector_string_normal,
        "x=array(STRING,0,2).append!([\"abc!@#中文 123\",\"\"]);x",
        DataForm::Vector,
        DataType::String,
        vector_build!(
            DolphinString,
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::String
    );
    macro_test_run_script!(
        test_run_script_vector_string_empty,
        "array(STRING)",
        DataForm::Vector,
        DataType::String,
        Vector::<DolphinString>::new(),
        ConstantImpl::Vector,
        VectorImpl::String
    );
    macro_test_run_script!(
        test_run_script_vector_string_all_null,
        "x=array(STRING,0,3).append!([\"\",\"\",\"\"]);x",
        DataForm::Vector,
        DataType::String,
        vector_build!(
            DolphinString,
            DolphinString::default(),
            DolphinString::default(),
            DolphinString::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::String
    );
    // blob
    macro_test_run_script!(
        test_run_script_vector_blob_normal,
        "x=array(BLOB,0,2).append!(blob([\"abc!@#中文 123\",\"\"]));x",
        DataForm::Vector,
        DataType::Blob,
        vector_build!(
            Blob,
            Blob::new("abc!@#中文 123".as_bytes().to_vec()),
            Blob::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Blob
    );
    macro_test_run_script!(
        test_run_script_vector_blob_empty,
        "array(BLOB)",
        DataForm::Vector,
        DataType::Blob,
        Vector::<Blob>::new(),
        ConstantImpl::Vector,
        VectorImpl::Blob
    );
    macro_test_run_script!(
        test_run_script_vector_blob_all_null,
        "x=array(BLOB,0,3).append!([\"\",\"\",\"\"]);x",
        DataForm::Vector,
        DataType::Blob,
        vector_build!(Blob, Blob::default(), Blob::default(), Blob::default()),
        ConstantImpl::Vector,
        VectorImpl::Blob
    );
    // decimal32
    macro_test_run_script!(
        test_run_script_vector_decimal32_normal,
        "x=array(DECIMAL32(3),0,3).append!(decimal32(['3.14','3.14159',NULL],3));x",
        DataForm::Vector,
        DataType::Decimal32,
        vector_build!(
            Decimal32,
            Decimal32::from_raw(3140i32, 3).unwrap(),
            Decimal32::from_raw(3142i32, 3).unwrap(),
            Decimal32::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Decimal32
    );
    macro_test_run_script!(
        test_run_script_vector_decimal32_empty,
        "array(DECIMAL32(3))",
        DataForm::Vector,
        DataType::Decimal32,
        Vector::<Decimal32>::new(),
        ConstantImpl::Vector,
        VectorImpl::Decimal32
    );
    macro_test_run_script!(
        test_run_script_vector_decimal32_all_null,
        "x=array(DECIMAL32(3),0,3).append!([decimal32(NULL,3),decimal32(NULL,3),decimal32(NULL,3)]);x",
        DataForm::Vector,
        DataType::Decimal32,
        vector_build!(
            Decimal32,
            Decimal32::default(),
            Decimal32::default(),
            Decimal32::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Decimal32
    );
    // decimal64
    macro_test_run_script!(
        test_run_script_vector_decimal64_normal,
        "x=array(DECIMAL64(3),0,3).append!(decimal64(['3.14','3.14159',NULL],3));x",
        DataForm::Vector,
        DataType::Decimal64,
        vector_build!(
            Decimal64,
            Decimal64::from_raw(3140i64, 3).unwrap(),
            Decimal64::from_raw(3142i64, 3).unwrap(),
            Decimal64::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Decimal64
    );
    macro_test_run_script!(
        test_run_script_vector_decimal64_empty,
        "array(DECIMAL64(3))",
        DataForm::Vector,
        DataType::Decimal64,
        Vector::<Decimal64>::new(),
        ConstantImpl::Vector,
        VectorImpl::Decimal64
    );
    macro_test_run_script!(
        test_run_script_vector_decimal64_all_null,
        "x=array(DECIMAL64(3),0,3).append!([decimal64(NULL,3),decimal64(NULL,3),decimal64(NULL,3)]);x",
        DataForm::Vector,
        DataType::Decimal64,
        vector_build!(
            Decimal64,
            Decimal64::default(),
            Decimal64::default(),
            Decimal64::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Decimal64
    );
    // decimal128
    macro_test_run_script!(
        test_run_script_vector_decimal128_normal,
        "x=array(DECIMAL128(3),0,3).append!(decimal128(['3.14','3.14159',NULL],3));x",
        DataForm::Vector,
        DataType::Decimal128,
        vector_build!(
            Decimal128,
            Decimal128::from_raw(3140i128, 3).unwrap(),
            Decimal128::from_raw(3142i128, 3).unwrap(),
            Decimal128::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Decimal128
    );
    macro_test_run_script!(
        test_run_script_vector_decimal128_empty,
        "array(DECIMAL128(3))",
        DataForm::Vector,
        DataType::Decimal128,
        Vector::<Decimal128>::new(),
        ConstantImpl::Vector,
        VectorImpl::Decimal128
    );
    macro_test_run_script!(
        test_run_script_vector_decimal128_all_null,
        "x=array(DECIMAL128(3),0,3).append!([decimal128(NULL,3),decimal128(NULL,3),decimal128(NULL,3)]);x",
        DataForm::Vector,
        DataType::Decimal128,
        vector_build!(
            Decimal128,
            Decimal128::default(),
            Decimal128::default(),
            Decimal128::default()
        ),
        ConstantImpl::Vector,
        VectorImpl::Decimal128
    );
    // any
    macro_test_run_script!(
        test_run_script_vector_any_normal,
        "(1,2,3)",
        DataForm::Vector,
        DataType::Any,
        vector_build!(
            Any,
            Any::new(Int::new(1i32).into()),
            Any::new(Int::new(2i32).into()),
            Any::new(Int::new(3i32).into())
        ),
        ConstantImpl::Vector,
        VectorImpl::Any
    );
    macro_test_run_script!(
        test_run_script_vector_any_special,
        "(1,[1],set([1]),dict([1],[1]),table([1] as `a))",
        DataForm::Vector,
        DataType::Any,
        vector_build!(
            Any,
            Any::new(Int::new(1i32).into()),
            Any::new(vector_build!(Int, Int::new(1i32)).into()),
            Any::new(SetImpl::Int(set_build!(Int, Int::new(1i32))).into()),
            Any::new(
                DictionaryImpl::Int(dictionary_build!(Int,Int::new(1i32) => Int::new(1i32))).into()
            ),
            Any::new(
                table_build!(
                    String::from("a") => vector_build!(
                        Int,
                        Int::new(1i32)
                    )
                )
                .into()
            )
        ),
        ConstantImpl::Vector,
        VectorImpl::Any
    );
    macro_test_run_script!(
        test_run_script_vector_any_empty,
        "array(ANY)",
        DataForm::Vector,
        DataType::Any,
        Vector::<Any>::new(),
        ConstantImpl::Vector,
        VectorImpl::Any
    );
    // todo:array_vector
}

// todo:matrix

mod test_run_script_set {
    use super::*;

    // char
    macro_test_run_script!(
        test_run_script_set_char_normal,
        "set(0c 127c -127c 00c)",
        DataForm::Set,
        DataType::Char,
        set_build!(
            Char,
            Char::new(0i8),
            Char::new(127i8),
            Char::new(-127i8),
            Char::default()
        ),
        ConstantImpl::Set,
        SetImpl::Char
    );
    macro_test_run_script!(
        test_run_script_set_char_empty,
        "set(array(CHAR))",
        DataForm::Set,
        DataType::Char,
        Set::<Char>::new(),
        ConstantImpl::Set,
        SetImpl::Char
    );
    // short
    macro_test_run_script!(
        test_run_script_set_short_normal,
        "set(0h 32767h -32767h 00h)",
        DataForm::Set,
        DataType::Short,
        set_build!(
            Short,
            Short::new(0i16),
            Short::new(32767i16),
            Short::new(-32767i16),
            Short::default()
        ),
        ConstantImpl::Set,
        SetImpl::Short
    );
    macro_test_run_script!(
        test_run_script_set_short_empty,
        "set(array(SHORT))",
        DataForm::Set,
        DataType::Short,
        Set::<Short>::new(),
        ConstantImpl::Set,
        SetImpl::Short
    );
    // int
    macro_test_run_script!(
        test_run_script_set_int_normal,
        "set(0i 2147483647i -2147483647i 00i)",
        DataForm::Set,
        DataType::Int,
        set_build!(
            Int,
            Int::new(0i32),
            Int::new(2147483647i32),
            Int::new(-2147483647i32),
            Int::default()
        ),
        ConstantImpl::Set,
        SetImpl::Int
    );
    macro_test_run_script!(
        test_run_script_set_int_empty,
        "set(array(INT))",
        DataForm::Set,
        DataType::Int,
        Set::<Int>::new(),
        ConstantImpl::Set,
        SetImpl::Int
    );
    // long
    macro_test_run_script!(
        test_run_script_set_long_normal,
        "set(0l 9223372036854775807l -9223372036854775807l 00l)",
        DataForm::Set,
        DataType::Long,
        set_build!(
            Long,
            Long::new(0i64),
            Long::new(9223372036854775807i64),
            Long::new(-9223372036854775807i64),
            Long::default()
        ),
        ConstantImpl::Set,
        SetImpl::Long
    );
    macro_test_run_script!(
        test_run_script_set_long_empty,
        "set(array(LONG))",
        DataForm::Set,
        DataType::Long,
        Set::<Long>::new(),
        ConstantImpl::Set,
        SetImpl::Long
    );
    // date
    macro_test_run_script!(
        test_run_script_set_date_normal,
        "set(1970.01.01d 2022.05.20d 00d)",
        DataForm::Set,
        DataType::Date,
        set_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap(),
            Date::from_ymd(2022, 5, 20).unwrap(),
            Date::default()
        ),
        ConstantImpl::Set,
        SetImpl::Date
    );
    macro_test_run_script!(
        test_run_script_set_date_empty,
        "set(array(DATE))",
        DataForm::Set,
        DataType::Date,
        Set::<Date>::new(),
        ConstantImpl::Set,
        SetImpl::Date
    );
    // month
    macro_test_run_script!(
        test_run_script_set_month_normal,
        "set(1970.01M 2022.05M 00M)",
        DataForm::Set,
        DataType::Month,
        set_build!(
            Month,
            Month::from_ym(1970, 1).unwrap(),
            Month::from_ym(2022, 5).unwrap(),
            Month::default()
        ),
        ConstantImpl::Set,
        SetImpl::Month
    );
    macro_test_run_script!(
        test_run_script_set_month_empty,
        "set(array(MONTH))",
        DataForm::Set,
        DataType::Month,
        Set::<Month>::new(),
        ConstantImpl::Set,
        SetImpl::Month
    );
    // time
    macro_test_run_script!(
        test_run_script_set_time_normal,
        "set(00:00:00.000t 13:50:59.123t 00t)",
        DataForm::Set,
        DataType::Time,
        set_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            Time::default()
        ),
        ConstantImpl::Set,
        SetImpl::Time
    );
    macro_test_run_script!(
        test_run_script_set_time_empty,
        "set(array(TIME))",
        DataForm::Set,
        DataType::Time,
        Set::<Time>::new(),
        ConstantImpl::Set,
        SetImpl::Time
    );
    // minute
    macro_test_run_script!(
        test_run_script_set_minute_normal,
        "set(00:00m 13:50m 00m)",
        DataForm::Set,
        DataType::Minute,
        set_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap(),
            Minute::from_hm(13, 50).unwrap(),
            Minute::default()
        ),
        ConstantImpl::Set,
        SetImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_set_minute_empty,
        "set(array(MINUTE))",
        DataForm::Set,
        DataType::Minute,
        Set::<Minute>::new(),
        ConstantImpl::Set,
        SetImpl::Minute
    );
    // second
    macro_test_run_script!(
        test_run_script_set_second_normal,
        "set(00:00:00 13:50:59 00s)",
        DataForm::Set,
        DataType::Second,
        set_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap(),
            Second::from_hms(13, 50, 59).unwrap(),
            Second::default()
        ),
        ConstantImpl::Set,
        SetImpl::Second
    );
    macro_test_run_script!(
        test_run_script_set_second_empty,
        "set(array(SECOND))",
        DataForm::Set,
        DataType::Second,
        Set::<Second>::new(),
        ConstantImpl::Set,
        SetImpl::Second
    );
    // datetime
    macro_test_run_script!(
        test_run_script_set_datetime_normal,
        "set(1970.01.01T00:00:00D 2022.05.20T13:50:59D 00D)",
        DataForm::Set,
        DataType::DateTime,
        set_build!(
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
        ),
        ConstantImpl::Set,
        SetImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_set_datetime_empty,
        "set(array(DATETIME))",
        DataForm::Set,
        DataType::DateTime,
        Set::<DateTime>::new(),
        ConstantImpl::Set,
        SetImpl::DateTime
    );
    // timestamp
    macro_test_run_script!(
        test_run_script_set_timestamp_normal,
        "set(1970.01.01T00:00:00.000T 2022.05.20T13:50:59.123T 00T)",
        DataForm::Set,
        DataType::Timestamp,
        set_build!(
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
        ),
        ConstantImpl::Set,
        SetImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_set_timestamp_empty,
        "set(array(TIMESTAMP))",
        DataForm::Set,
        DataType::Timestamp,
        Set::<Timestamp>::new(),
        ConstantImpl::Set,
        SetImpl::Timestamp
    );
    // nanotime
    macro_test_run_script!(
        test_run_script_set_nanotime_normal,
        "set(00:00:00.000000000n 13:50:59.123456789n 00n)",
        DataForm::Set,
        DataType::NanoTime,
        set_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            NanoTime::default()
        ),
        ConstantImpl::Set,
        SetImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_set_nanotime_empty,
        "set(array(NANOTIME))",
        DataForm::Set,
        DataType::NanoTime,
        Set::<NanoTime>::new(),
        ConstantImpl::Set,
        SetImpl::NanoTime
    );
    // nanotimestamp
    macro_test_run_script!(
        test_run_script_set_nanotimestamp_normal,
        "set(1970.01.01T00:00:00.000000000N 2022.05.20T13:50:59.123456789N 00N)",
        DataForm::Set,
        DataType::NanoTimestamp,
        set_build!(
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
        ),
        ConstantImpl::Set,
        SetImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_set_nanotimestamp_empty,
        "set(array(NANOTIMESTAMP))",
        DataForm::Set,
        DataType::NanoTimestamp,
        Set::<NanoTimestamp>::new(),
        ConstantImpl::Set,
        SetImpl::NanoTimestamp
    );
    // datehour
    macro_test_run_script!(
        test_run_script_set_datehour_normal,
        "set([datehour(\"1970.01.01T00\"),datehour(\"2022.05.20T13\"),datehour(NULL)])",
        DataForm::Set,
        DataType::DateHour,
        set_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DateHour::default()
        ),
        ConstantImpl::Set,
        SetImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_set_datehour_empty,
        "set(array(DATEHOUR))",
        DataForm::Set,
        DataType::DateHour,
        Set::<DateHour>::new(),
        ConstantImpl::Set,
        SetImpl::DateHour
    );
    // float
    macro_test_run_script!(
        test_run_script_set_float_normal,
        "set(3.14f 3.15f 00f)",
        DataForm::Set,
        DataType::Float,
        set_build!(
            Float,
            Float::new(3.14f32),
            Float::new(3.15f32),
            Float::default()
        ),
        ConstantImpl::Set,
        SetImpl::Float
    );
    macro_test_run_script!(
        test_run_script_set_float_empty,
        "set(array(FLOAT))",
        DataForm::Set,
        DataType::Float,
        Set::<Float>::new(),
        ConstantImpl::Set,
        SetImpl::Float
    );
    // double
    macro_test_run_script!(
        test_run_script_set_double_normal,
        "set(3.14F 3.15F 00F)",
        DataForm::Set,
        DataType::Double,
        set_build!(
            Double,
            Double::new(3.14f64),
            Double::new(3.15f64),
            Double::default()
        ),
        ConstantImpl::Set,
        SetImpl::Double
    );
    macro_test_run_script!(
        test_run_script_set_double_empty,
        "set(array(DOUBLE))",
        DataForm::Set,
        DataType::Double,
        Set::<Double>::new(),
        ConstantImpl::Set,
        SetImpl::Double
    );
    // symbol
    macro_test_run_script!(
        test_run_script_set_symbol_normal,
        "set(symbol(\"abc!@#中文 123\" \"abc!@#中文 124\" \"\"))",
        DataForm::Set,
        DataType::Symbol,
        set_build!(
            Symbol,
            Symbol::new(String::from("abc!@#中文 123")),
            Symbol::new(String::from("abc!@#中文 124")),
            Symbol::default()
        ),
        ConstantImpl::Set,
        SetImpl::Symbol
    );
    macro_test_run_script!(
        test_run_script_set_symbol_empty,
        "set(array(SYMBOL))",
        DataForm::Set,
        DataType::Symbol,
        Set::<Symbol>::new(),
        ConstantImpl::Set,
        SetImpl::Symbol
    );
    // string
    macro_test_run_script!(
        test_run_script_set_string_normal,
        "set(\"abc!@#中文 123\" \"abc!@#中文 124\" \"\")",
        DataForm::Set,
        DataType::String,
        set_build!(
            DolphinString,
            DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("abc!@#中文 124")),
            DolphinString::default()
        ),
        ConstantImpl::Set,
        SetImpl::String
    );
    macro_test_run_script!(
        test_run_script_set_string_empty,
        "set(array(STRING))",
        DataForm::Set,
        DataType::String,
        Set::<DolphinString>::new(),
        ConstantImpl::Set,
        SetImpl::String
    );
    // blob
    // todo:RUS-24
    // macro_test_run_script!(
    //     test_run_script_set_blob_normal,
    //     "set(blob(\"abc!@#中文 123\" \"abc!@#中文 124\" \"\"))",
    //     DataForm::Set,
    //     DataType::Blob,
    //     set_build!(
    //         Blob,
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         Blob::new("abc!@#中文 124".as_bytes().to_vec()),
    //         Blob::default()
    //     ),
    //     ConstantImpl::Set,
    //     SetImpl::Blob
    // );
    // macro_test_run_script!(
    //     test_run_script_set_blob_empty,
    //     "set(array(STRING))",
    //     DataForm::Set,
    //     DataType::Blob,
    //     Set::<Blob>::new(),
    //     ConstantImpl::Set,
    //     SetImpl::Blob
    // );
}

mod test_run_script_dictionary {
    use super::*;

    // char->bool
    macro_test_run_script!(
        test_run_script_dictionary_char_bool_normal,
        "dict([0c,127c,-127c,00c],[true,true,false,00b])",
        DataForm::Dictionary,
        DataType::Char,
        dictionary_build!(
            Char,
            Char::new(0i8) => Bool::new(true),
            Char::new(127i8) => Bool::new(true),
            Char::new(-127i8) => Bool::new(false),
            Char::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Char
    );
    macro_test_run_script!(
        test_run_script_dictionary_char_bool_empty,
        "dict([]$CHAR,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Char,
        Dictionary::<Char>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Char
    );
    // short->bool
    macro_test_run_script!(
        test_run_script_dictionary_short_bool_normal,
        "dict([32767h,-32767h,00h],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Short,
        dictionary_build!(
            Short,
            Short::new(32767i16) => Bool::new(true),
            Short::new(-32767i16) => Bool::new(false),
            Short::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Short
    );
    macro_test_run_script!(
        test_run_script_dictionary_short_bool_empty,
        "dict([]$SHORT,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Short,
        Dictionary::<Short>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Short
    );
    // int->bool
    macro_test_run_script!(
        test_run_script_dictionary_int_bool_normal,
        "dict([2147483647i,-2147483647i,00i],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Int,
        dictionary_build!(
            Int,
            Int::new(2147483647i32) => Bool::new(true),
            Int::new(-2147483647i32) => Bool::new(false),
            Int::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Int
    );
    macro_test_run_script!(
        test_run_script_dictionary_int_bool_empty,
        "dict([]$INT,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Int,
        Dictionary::<Int>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Int
    );
    // long->bool
    macro_test_run_script!(
        test_run_script_dictionary_long_bool_normal,
        "dict([9223372036854775807l,-9223372036854775807l,00l],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Long,
        dictionary_build!(
            Long,
            Long::new(9223372036854775807i64) => Bool::new(true),
            Long::new(-9223372036854775807i64) => Bool::new(false),
            Long::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Long
    );
    macro_test_run_script!(
        test_run_script_dictionary_long_bool_empty,
        "dict([]$LONG,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Long,
        Dictionary::<Long>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Long
    );
    // date->bool
    macro_test_run_script!(
        test_run_script_dictionary_date_bool_normal,
        "dict([1970.01.01d,2022.05.20d,00d],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Date,
        dictionary_build!(
            Date,
            Date::from_ymd(1970, 1, 1).unwrap() => Bool::new(true),
            Date::from_ymd(2022, 5, 20).unwrap() => Bool::new(false),
            Date::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Date
    );
    macro_test_run_script!(
        test_run_script_dictionary_date_bool_empty,
        "dict([]$DATE,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Date,
        Dictionary::<Date>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Date
    );
    // month->bool
    macro_test_run_script!(
        test_run_script_dictionary_month_bool_normal,
        "dict([1970.01M,2022.05M,00M],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Month,
        dictionary_build!(
            Month,
            Month::from_ym(1970, 1).unwrap() => Bool::new(true),
            Month::from_ym(2022, 5).unwrap() => Bool::new(false),
            Month::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Month
    );
    macro_test_run_script!(
        test_run_script_dictionary_month_bool_empty,
        "dict([]$MONTH,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Month,
        Dictionary::<Month>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Month
    );
    // time->bool
    macro_test_run_script!(
        test_run_script_dictionary_time_bool_normal,
        "dict([00:00:00.000t,13:50:59.123t,00t],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Time,
        dictionary_build!(
            Time,
            Time::from_hms_milli(0, 0, 0, 0).unwrap() => Bool::new(true),
            Time::from_hms_milli(13, 50, 59, 123).unwrap() => Bool::new(false),
            Time::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Time
    );
    macro_test_run_script!(
        test_run_script_dictionary_time_bool_empty,
        "dict([]$TIME,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Time,
        Dictionary::<Time>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Time
    );
    // minute->bool
    macro_test_run_script!(
        test_run_script_dictionary_minute_bool_normal,
        "dict([00:00m,13:50m,00m],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Minute,
        dictionary_build!(
            Minute,
            Minute::from_hm(0, 0).unwrap() => Bool::new(true),
            Minute::from_hm(13, 50).unwrap() => Bool::new(false),
            Minute::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Minute
    );
    macro_test_run_script!(
        test_run_script_dictionary_minute_bool_empty,
        "dict([]$MINUTE,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Minute,
        Dictionary::<Minute>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Minute
    );
    // second->bool
    macro_test_run_script!(
        test_run_script_dictionary_second_bool_normal,
        "dict([00:00:00s,13:50:59s,00s],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Second,
        dictionary_build!(
            Second,
            Second::from_hms(0, 0, 0).unwrap() => Bool::new(true),
            Second::from_hms(13, 50, 59).unwrap() => Bool::new(false),
            Second::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Second
    );
    macro_test_run_script!(
        test_run_script_dictionary_second_bool_empty,
        "dict([]$SECOND,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Second,
        Dictionary::<Second>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Second
    );
    // datetime->bool
    macro_test_run_script!(
        test_run_script_dictionary_datetime_bool_normal,
        "dict([1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D],[true,false,00b])",
        DataForm::Dictionary,
        DataType::DateTime,
        dictionary_build!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::DateTime
    );
    macro_test_run_script!(
        test_run_script_dictionary_datetime_bool_empty,
        "dict([]$DATETIME,[]$BOOL)",
        DataForm::Dictionary,
        DataType::DateTime,
        Dictionary::<DateTime>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::DateTime
    );
    // timestamp->bool
    macro_test_run_script!(
        test_run_script_dictionary_timestamp_bool_normal,
        "dict([1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Timestamp,
        dictionary_build!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Timestamp
    );
    macro_test_run_script!(
        test_run_script_dictionary_timestamp_bool_empty,
        "dict([]$TIMESTAMP,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Timestamp,
        Dictionary::<Timestamp>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Timestamp
    );
    // nanotime->bool
    macro_test_run_script!(
        test_run_script_dictionary_nanotime_bool_normal,
        "dict([00:00:00.000000000n,13:50:59.123456789,00n],[true,false,00b])",
        DataForm::Dictionary,
        DataType::NanoTime,
        dictionary_build!(
            NanoTime,
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap() => Bool::new(true),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap() => Bool::new(false),
            NanoTime::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::NanoTime
    );
    macro_test_run_script!(
        test_run_script_dictionary_nanotime_bool_empty,
        "dict([]$NANOTIME,[]$BOOL)",
        DataForm::Dictionary,
        DataType::NanoTime,
        Dictionary::<NanoTime>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::NanoTime
    );
    // nanotimestamp->bool
    macro_test_run_script!(
        test_run_script_dictionary_nanotimestamp_bool_normal,
        "dict([1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N],[true,false,00b])",
        DataForm::Dictionary,
        DataType::NanoTimestamp,
        dictionary_build!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::NanoTimestamp
    );
    macro_test_run_script!(
        test_run_script_dictionary_nanotimestamp_bool_empty,
        "dict([]$NANOTIMESTAMP,[]$BOOL)",
        DataForm::Dictionary,
        DataType::NanoTimestamp,
        Dictionary::<NanoTimestamp>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::NanoTimestamp
    );
    // datehour->bool
    macro_test_run_script!(
        test_run_script_dictionary_datehour_bool_normal,
        "dict(datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]),[true,false,00b])",
        DataForm::Dictionary,
        DataType::DateHour,
        dictionary_build!(
            DateHour,
            DateHour::from_ymd_h(1970, 1, 1, 0).unwrap() => Bool::new(true),
            DateHour::from_ymd_h(2022, 5, 20, 13).unwrap() => Bool::new(false),
            DateHour::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::DateHour
    );
    macro_test_run_script!(
        test_run_script_dictionary_datehour_bool_empty,
        "dict([]$DATEHOUR,[]$BOOL)",
        DataForm::Dictionary,
        DataType::DateHour,
        Dictionary::<DateHour>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::DateHour
    );
    // float->bool
    macro_test_run_script!(
        test_run_script_dictionary_float_bool_normal,
        "dict([3.14f,3.15f,00f],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Float,
        dictionary_build!(
            Float,
            Float::new(3.14f32) => Bool::new(true),
            Float::new(3.15f32) => Bool::new(false),
            Float::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Float
    );
    macro_test_run_script!(
        test_run_script_dictionary_float_bool_empty,
        "dict([]$FLOAT,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Float,
        Dictionary::<Float>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Float
    );
    // double->bool
    macro_test_run_script!(
        test_run_script_dictionary_double_bool_normal,
        "dict([3.14F,3.15F,00F],[true,false,00b])",
        DataForm::Dictionary,
        DataType::Double,
        dictionary_build!(
            Double,
            Double::new(3.14f64) => Bool::new(true),
            Double::new(3.15f64) => Bool::new(false),
            Double::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Double
    );
    macro_test_run_script!(
        test_run_script_dictionary_double_bool_empty,
        "dict([]$DOUBLE,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Double,
        Dictionary::<Double>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Double
    );
    // symbol->bool
    macro_test_run_script!(
        test_run_script_dictionary_symbol_bool_normal,
        "dict(symbol([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]),[true,false,00b])",
        DataForm::Dictionary,
        DataType::Symbol,
        dictionary_build!(
            Symbol,
            Symbol::new(String::from("abc!@#中文 123")) => Bool::new(true),
            Symbol::new(String::from("abc!@#中文 124")) => Bool::new(false),
            Symbol::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::Symbol
    );
    macro_test_run_script!(
        test_run_script_dictionary_symbol_bool_empty,
        "dict([]$STRING$SYMBOL,[]$BOOL)",
        DataForm::Dictionary,
        DataType::Symbol,
        Dictionary::<Symbol>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::Symbol
    );
    // string->bool
    macro_test_run_script!(
        test_run_script_dictionary_string_bool_normal,
        "dict([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"],[true,false,00b])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("abc!@#中文 123")) => Bool::new(true),
            DolphinString::new(String::from("abc!@#中文 124")) => Bool::new(false),
            DolphinString::default() => Bool::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    macro_test_run_script!(
        test_run_script_dictionary_string_bool_empty,
        "dict([]$STRING,[]$BOOL)",
        DataForm::Dictionary,
        DataType::String,
        Dictionary::<DolphinString>::new(),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // blob->bool
    // todo:RUS-25
    // macro_test_run_script!(
    //     test_run_script_dictionary_blob_bool_normal,
    //     "dict(blob([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]),[true,false,00b])",
    //     DataForm::Dictionary,
    //     DataType::Blob,
    //     dictionary_build!(
    //         Blob,
    //         Blob::new("abc!@#中文 123".as_bytes().to_vec()) => Bool::new(true),
    //         Blob::new("abc!@#中文 124".as_bytes().to_vec()) => Bool::new(false),
    //         Blob::default() => Bool::default()
    //     ),
    //     ConstantImpl::Dictionary,
    //     DictionaryImpl::Blob
    // );
    // macro_test_run_script!(
    //     test_run_script_dictionary_blob_bool_empty,
    //     "dict([]$BLOB,[]$BOOL)",
    //     DataForm::Dictionary,
    //     DataType::Blob,
    //     Dictionary::<Blob>::new(),
    //     ConstantImpl::Dictionary,
    //     DictionaryImpl::Blob
    // );
    // string->char
    macro_test_run_script!(
        test_run_script_dictionary_string_char,
        "dict([\"1\",\"2\",\"\"],[127c,-127c,00c])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Char::new(127i8),
            DolphinString::new(String::from("2")) => Char::new(-127i8),
            DolphinString::default() => Char::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->short
    macro_test_run_script!(
        test_run_script_dictionary_string_short,
        "dict([\"1\",\"2\",\"\"],[32767h,-32767h,00h])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Short::new(32767i16),
            DolphinString::new(String::from("2")) => Short::new(-32767i16),
            DolphinString::default() => Short::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->int
    macro_test_run_script!(
        test_run_script_dictionary_string_int,
        "dict([\"1\",\"2\",\"\"],[2147483647i,-2147483647i,00i])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Int::new(2147483647i32),
            DolphinString::new(String::from("2")) => Int::new(-2147483647i32),
            DolphinString::default() => Int::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->long
    macro_test_run_script!(
        test_run_script_dictionary_string_long,
        "dict([\"1\",\"2\",\"\"],[9223372036854775807l,-9223372036854775807l,00l])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Long::new(9223372036854775807i64),
            DolphinString::new(String::from("2")) => Long::new(-9223372036854775807i64),
            DolphinString::default() => Long::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->date
    macro_test_run_script!(
        test_run_script_dictionary_string_date,
        "dict([\"1\",\"2\",\"\"],[1970.01.01d,2022.05.20d,00d])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Date::from_ymd(1970, 1, 1).unwrap(),
            DolphinString::new(String::from("2")) => Date::from_ymd(2022, 5, 20).unwrap(),
            DolphinString::default() => Date::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->month
    macro_test_run_script!(
        test_run_script_dictionary_string_month,
        "dict([\"1\",\"2\",\"\"],[1970.01M,2022.05M,00M])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Month::from_ym(1970, 1).unwrap(),
            DolphinString::new(String::from("2")) => Month::from_ym(2022, 5).unwrap(),
            DolphinString::default() => Month::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->time
    macro_test_run_script!(
        test_run_script_dictionary_string_time,
        "dict([\"1\",\"2\",\"\"],[00:00:00.000t,13:50:59.123t,00t])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Time::from_hms_milli(0, 0, 0, 0).unwrap(),
            DolphinString::new(String::from("2")) => Time::from_hms_milli(13, 50, 59, 123).unwrap(),
            DolphinString::default() => Time::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->minute
    macro_test_run_script!(
        test_run_script_dictionary_string_minute,
        "dict([\"1\",\"2\",\"\"],[00:00m,13:50m,00m])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Minute::from_hm(0, 0).unwrap(),
            DolphinString::new(String::from("2")) => Minute::from_hm(13, 50).unwrap(),
            DolphinString::default() => Minute::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->second
    macro_test_run_script!(
        test_run_script_dictionary_string_second,
        "dict([\"1\",\"2\",\"\"],[00:00:00s,13:50:59s,00s])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Second::from_hms(0, 0, 0).unwrap(),
            DolphinString::new(String::from("2")) => Second::from_hms(13, 50, 59).unwrap(),
            DolphinString::default() => Second::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->datetime
    macro_test_run_script!(
        test_run_script_dictionary_string_datetime,
        "dict([\"1\",\"2\",\"\"],[1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->timestamp
    macro_test_run_script!(
        test_run_script_dictionary_string_timestamp,
        "dict([\"1\",\"2\",\"\"],[1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->nanotime
    macro_test_run_script!(
        test_run_script_dictionary_string_nanotime,
        "dict([\"1\",\"2\",\"\"],[00:00:00.000000000n,13:50:59.123456789n,00n])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
            DolphinString::new(String::from("2")) => NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
            DolphinString::default() => NanoTime::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->nanotimestamp
    macro_test_run_script!(
        test_run_script_dictionary_string_nanotimestamp,
        "dict([\"1\",\"2\",\"\"],[1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->datehour
    macro_test_run_script!(
        test_run_script_dictionary_string_datehour,
        "dict([\"1\",\"2\",\"\"],datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
            DolphinString::new(String::from("2")) => DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
            DolphinString::default() => DateHour::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->float
    macro_test_run_script!(
        test_run_script_dictionary_string_float,
        "dict([\"1\",\"2\",\"\"],[3.14f,3.15f,00f])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Float::new(3.14f32),
            DolphinString::new(String::from("2")) => Float::new(3.15f32),
            DolphinString::default() => Float::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->double
    macro_test_run_script!(
        test_run_script_dictionary_string_double,
        "dict([\"1\",\"2\",\"\"],[3.14F,3.15F,00F])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Double::new(3.14f64),
            DolphinString::new(String::from("2")) => Double::new(3.15f64),
            DolphinString::default() => Double::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->symbol
    macro_test_run_script!(
        test_run_script_dictionary_string_symbol,
        "dict([\"1\",\"2\",\"\"],symbol([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Symbol::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("2")) => Symbol::new(String::from("abc!@#中文 124")),
            DolphinString::default() => Symbol::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->string
    macro_test_run_script!(
        test_run_script_dictionary_string_string,
        "dict([\"1\",\"2\",\"\"],[\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"])",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => DolphinString::new(String::from("abc!@#中文 123")),
            DolphinString::new(String::from("2")) => DolphinString::new(String::from("abc!@#中文 124")),
            DolphinString::default() => DolphinString::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->blob
    // todo:RUS-25
    // macro_test_run_script!(
    //     test_run_script_dictionary_string_blob,
    //     "dict([\"1\",\"2\",\"\"],blob([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]))",
    //     DataForm::Dictionary,
    //     DataType::String,
    //     dictionary_build!(
    //         DolphinString,
    //         DolphinString::new(String::from("1")) => Blob::new("abc!@#中文 123".as_bytes().to_vec()),
    //         DolphinString::new(String::from("2")) => Blob::new("abc!@#中文 124".as_bytes().to_vec()),
    //         DolphinString::default() => Blob::default()
    //     ),
    //     ConstantImpl::Dictionary,
    //     DictionaryImpl::String
    // );
    // string->decimal32
    macro_test_run_script!(
        test_run_script_dictionary_string_decimal32,
        "dict([\"1\",\"2\",\"\"],decimal32([\"3.14\",\"3.15\",NULL],2))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Decimal32::from_raw(314i32, 2).unwrap(),
            DolphinString::new(String::from("2")) => Decimal32::from_raw(315i32, 2).unwrap(),
            DolphinString::default() => Decimal32::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->decimal64
    macro_test_run_script!(
        test_run_script_dictionary_string_decimal64,
        "dict([\"1\",\"2\",\"\"],decimal64([\"3.14\",\"3.15\",NULL],2))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Decimal64::from_raw(314i64, 2).unwrap(),
            DolphinString::new(String::from("2")) => Decimal64::from_raw(315i64, 2).unwrap(),
            DolphinString::default() => Decimal64::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->decimal128
    macro_test_run_script!(
        test_run_script_dictionary_string_decimal128,
        "dict([\"1\",\"2\",\"\"],decimal128([\"3.14\",\"3.15\",NULL],2))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Decimal128::from_raw(314i128, 2).unwrap(),
            DolphinString::new(String::from("2")) => Decimal128::from_raw(315i128, 2).unwrap(),
            DolphinString::default() => Decimal128::default()
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    // string->any
    macro_test_run_script!(
        test_run_script_dictionary_string_any_normal,
        "dict([\"1\",\"2\",\"\"],(1c,2h,3i))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build!(
            DolphinString,
            DolphinString::new(String::from("1")) => Char::new(1i8),
            DolphinString::new(String::from("2")) => Short::new(2i16),
            DolphinString::default() => Int::new(3i32)
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
    macro_test_run_script!(
        test_run_script_dictionary_string_any_special,
        "dict([\"1\",\"2\",\"3\",\"4\",\"5\"],(1,[1],set([1]),dict([1],[1]),table([1] as `a)))",
        DataForm::Dictionary,
        DataType::String,
        dictionary_build_any!(
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
        ),
        ConstantImpl::Dictionary,
        DictionaryImpl::String
    );
}

mod test_run_script_table {
    use super::*;

    macro_rules! macro_test_run_script_table {
        ($name:ident, $script:expr, $expect_value:expr) => {
            #[tokio::test]
            async fn $name() {
                // connect
                let conf = Config::new();
                let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
                builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
                let mut client = builder.connect().await.unwrap();
                // run_script
                let res = client.run_script($script).await.unwrap();
                // assert
                assert!(res.is_some());
                let res_constantimpl = res.unwrap();
                println!("{}", res_constantimpl);
                assert_eq!(res_constantimpl.data_form(), DataForm::Table);
                assert_eq!(res_constantimpl.data_type(), DataType::AnyDictionary);
                if let ConstantImpl::Table(res_) = res_constantimpl {
                    assert_eq!(res_, $expect_value);
                } else {
                    assert!(false, "error in constant");
                }
            }
        };
    }

    // bool
    macro_test_run_script_table!(
        test_run_script_table_bool_normal,
        "table([true,false,00b] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Bool,
                Bool::new(true),
                Bool::new(false),
                Bool::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_bool_empty,
        "table([]$BOOL as `a)",
        table_build!(
            String::from("a") => Vector::<Bool>::new()
        )
    );
    // char
    macro_test_run_script_table!(
        test_run_script_table_char_normal,
        "table([127c,-127c,0c,00c] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Char,
                Char::new(127i8),
                Char::new(-127i8),
                Char::new(0i8),
                Char::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_char_empty,
        "table([]$CHAR as `a)",
        table_build!(
            String::from("a") => Vector::<Char>::new()
        )
    );
    // short
    macro_test_run_script_table!(
        test_run_script_table_short_normal,
        "table([32767h,-32767h,0h,00h] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Short,
                Short::new(32767i16),
                Short::new(-32767i16),
                Short::new(0i16),
                Short::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_short_empty,
        "table([]$SHORT as `a)",
        table_build!(
            String::from("a") => Vector::<Short>::new()
        )
    );
    // int
    macro_test_run_script_table!(
        test_run_script_table_int_normal,
        "table([2147483647i,-2147483647i,0i,00i] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_int_empty,
        "table([]$INT as `a)",
        table_build!(
            String::from("a") => Vector::<Int>::new()
        )
    );
    // long
    macro_test_run_script_table!(
        test_run_script_table_long_normal,
        "table([9223372036854775807l,-9223372036854775807l,0l,00l] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Long,
                Long::new(9223372036854775807i64),
                Long::new(-9223372036854775807i64),
                Long::new(0i64),
                Long::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_long_empty,
        "table([]$LONG as `a)",
        table_build!(
            String::from("a") => Vector::<Long>::new()
        )
    );
    // date
    macro_test_run_script_table!(
        test_run_script_table_date_normal,
        "table([1970.01.01d,2022.05.20d,00d] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Date,
                Date::from_ymd(1970, 1, 1).unwrap(),
                Date::from_ymd(2022, 5, 20).unwrap(),
                Date::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_date_empty,
        "table([]$DATE as `a)",
        table_build!(
            String::from("a") => Vector::<Date>::new()
        )
    );
    // month
    macro_test_run_script_table!(
        test_run_script_table_month_normal,
        "table([1970.01M,2022.05M,00M] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Month,
                Month::from_ym(1970, 1).unwrap(),
                Month::from_ym(2022, 5).unwrap(),
                Month::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_month_empty,
        "table([]$MONTH as `a)",
        table_build!(
            String::from("a") => Vector::<Month>::new()
        )
    );
    // time
    macro_test_run_script_table!(
        test_run_script_table_time_normal,
        "table([00:00:00.000t,13:50:59.123t,00t] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Time,
                Time::from_hms_milli(0, 0, 0, 0).unwrap(),
                Time::from_hms_milli(13, 50, 59, 123).unwrap(),
                Time::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_time_empty,
        "table([]$TIME as `a)",
        table_build!(
            String::from("a") => Vector::<Time>::new()
        )
    );
    // minute
    macro_test_run_script_table!(
        test_run_script_table_minute_normal,
        "table([00:00m,13:50m,00m] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Minute,
                Minute::from_hm(0, 0).unwrap(),
                Minute::from_hm(13, 50).unwrap(),
                Minute::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_minute_empty,
        "table([]$MINUTE as `a)",
        table_build!(
            String::from("a") => Vector::<Minute>::new()
        )
    );
    // second
    macro_test_run_script_table!(
        test_run_script_table_second_normal,
        "table([00:00:00s,13:50:59s,00s] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Second,
                Second::from_hms(0, 0, 0).unwrap(),
                Second::from_hms(13, 50, 59).unwrap(),
                Second::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_second_empty,
        "table([]$SECOND as `a)",
        table_build!(
            String::from("a") => Vector::<Second>::new()
        )
    );
    // datetime
    macro_test_run_script_table!(
        test_run_script_table_datetime_normal,
        "table([1970.01.01T00:00:00D,2022.05.20T13:50:59D,00D] as `a)",
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
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_datetime_empty,
        "table([]$DATETIME as `a)",
        table_build!(
            String::from("a") => Vector::<DateTime>::new()
        )
    );
    // timestamp
    macro_test_run_script_table!(
        test_run_script_table_timestamp_normal,
        "table([1970.01.01T00:00:00.000T,2022.05.20T13:50:59.123T,00T] as `a)",
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
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_timestamp_empty,
        "table([]$TIMESTAMP as `a)",
        table_build!(
            String::from("a") => Vector::<Timestamp>::new()
        )
    );
    // nanotime
    macro_test_run_script_table!(
        test_run_script_table_nanotime_normal,
        "table([00:00:00.000000000n,13:50:59.123456789n,00n] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                NanoTime,
                NanoTime::from_hms_nano(0, 0, 0, 0).unwrap(),
                NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap(),
                NanoTime::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_nanotime_empty,
        "table([]$NANOTIME as `a)",
        table_build!(
            String::from("a") => Vector::<NanoTime>::new()
        )
    );
    // nanotimestamp
    macro_test_run_script_table!(
        test_run_script_table_nanotimestamp_normal,
        "table([1970.01.01T00:00:00.000000000N,2022.05.20T13:50:59.123456789N,00N] as `a)",
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
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_nanotimestamp_empty,
        "table([]$NANOTIMESTAMP as `a)",
        table_build!(
            String::from("a") => Vector::<NanoTimestamp>::new()
        )
    );
    // datehour
    macro_test_run_script_table!(
        test_run_script_table_datehour_normal,
        "table(datehour([\"1970.01.01T00\",\"2022.05.20T13\",NULL]) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                DateHour,
                DateHour::from_ymd_h(1970, 1, 1, 0).unwrap(),
                DateHour::from_ymd_h(2022, 5, 20, 13).unwrap(),
                DateHour::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_datehour_empty,
        "table([]$DATEHOUR as `a)",
        table_build!(
            String::from("a") => Vector::<DateHour>::new()
        )
    );
    // float
    macro_test_run_script_table!(
        test_run_script_table_float_normal,
        "table([3.14f,3.15f,00f] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Float,
                Float::new(3.14f32),
                Float::new(3.15f32),
                Float::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_float_empty,
        "table([]$FLOAT as `a)",
        table_build!(
            String::from("a") => Vector::<Float>::new()
        )
    );
    // double
    macro_test_run_script_table!(
        test_run_script_table_double_normal,
        "table([3.14F,3.15F,00F] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Double,
                Double::new(3.14f64),
                Double::new(3.15f64),
                Double::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_double_empty,
        "table([]$DOUBLE as `a)",
        table_build!(
            String::from("a") => Vector::<Double>::new()
        )
    );
    // symbol
    macro_test_run_script_table!(
        test_run_script_table_symbol_normal,
        "table(symbol([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Symbol,
                Symbol::new(String::from("abc!@#中文 123")),
                Symbol::new(String::from("abc!@#中文 124")),
                Symbol::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_symbol_empty,
        "table([]$STRING$SYMBOL as `a)",
        table_build!(
            String::from("a") => Vector::<Symbol>::new()
        )
    );
    // string
    macro_test_run_script_table!(
        test_run_script_table_string_normal,
        "table([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"] as `a)",
        table_build!(
            String::from("a") => vector_build!(
                DolphinString,
                DolphinString::new(String::from("abc!@#中文 123")),
                DolphinString::new(String::from("abc!@#中文 124")),
                DolphinString::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_string_empty,
        "table([]$STRING as `a)",
        table_build!(
            String::from("a") => Vector::<DolphinString>::new()
        )
    );
    // blob
    macro_test_run_script_table!(
        test_run_script_table_blob_normal,
        "table(blob([\"abc!@#中文 123\",\"abc!@#中文 124\",\"\"]) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Blob,
                Blob::new("abc!@#中文 123".as_bytes().to_vec()),
                Blob::new("abc!@#中文 124".as_bytes().to_vec()),
                Blob::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_blob_empty,
        "table([]$BLOB as `a)",
        table_build!(
            String::from("a") => Vector::<Blob>::new()
        )
    );
    // decimal32
    macro_test_run_script_table!(
        test_run_script_table_decimal32_normal,
        "table(decimal32([\"3.14\",\"3.15\",NULL],2) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Decimal32,
                Decimal32::from_raw(314i32,2).unwrap(),
                Decimal32::from_raw(315i32,2).unwrap(),
                Decimal32::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_decimal32_empty,
        "table([]$DECIMAL32(0) as `a)",
        table_build!(
            String::from("a") => Vector::<Decimal32>::new()
        )
    );
    // decimal64
    macro_test_run_script_table!(
        test_run_script_table_decimal64_normal,
        "table(decimal64([\"3.14\",\"3.15\",NULL],2) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Decimal64,
                Decimal64::from_raw(314i64,2).unwrap(),
                Decimal64::from_raw(315i64,2).unwrap(),
                Decimal64::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_decimal64_empty,
        "table([]$DECIMAL64(0) as `a)",
        table_build!(
            String::from("a") => Vector::<Decimal64>::new()
        )
    );
    // decimal128
    macro_test_run_script_table!(
        test_run_script_table_decimal128_normal,
        "table(decimal128([\"3.14\",\"3.15\",NULL],2) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Decimal128,
                Decimal128::from_raw(314i128,2).unwrap(),
                Decimal128::from_raw(315i128,2).unwrap(),
                Decimal128::default()
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_decimal128_empty,
        "table([]$DECIMAL128(0) as `a)",
        table_build!(
            String::from("a") => Vector::<Decimal128>::new()
        )
    );

    // any
    macro_test_run_script_table!(
        test_run_script_table_any_normal,
        "table((1,2,3) as `a)",
        table_build!(
            String::from("a") => vector_build!(
                Any,
                Any::new(Int::new(1i32).into()),
                Any::new(Int::new(2i32).into()),
                Any::new(Int::new(3i32).into())
            )
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_any_special,
        "table((1,[1],set([1]),dict([1],[1]),table([1] as `a)) as `a)",
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
        )
    );
    macro_test_run_script_table!(
        test_run_script_table_any_empty,
        "table([]$ANY as `a)",
        table_build!(
            String::from("a") => Vector::<Any>::new()
        )
    );
    // todo:array vector table
}

// todo:RUS-44
#[tokio::test]
async fn test_run_script_print() {
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    let x = client.run_script("print 1+1").await;
    println!("{x:?}");
}

// todo:RUS-63
#[tokio::test]
#[should_panic]
async fn test_run_script_not_support_type() {
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    let _ = client.run_script("< 1+1 >").await;
}

#[tokio::test]
async fn test_run_script_not_support_form() {
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    assert!(client.run_script("1..4$2:2").await.is_err());
}
