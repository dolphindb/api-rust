mod setup;
mod utils;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dolphindb::client::ClientBuilder;
use dolphindb::types::*;
use encoding::{all::GBK, EncoderTrap, Encoding};
use rstest::rstest;
use rust_decimal::Decimal;
use setup::settings::Config;
use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash};
use std::str::FromStr;
use std::vec;

mod test_types_func_type {
    use super::*;

    macro_rules! macro_test_is_null {
        ($name:ident, $type:ty, $($value:expr => $expect:expr),*) => {
            #[rstest]
            $(
                #[case($value, $expect)]
            )*
            fn $name(#[case] value:$type, #[case] expect:bool){
                assert_eq!(value.is_null(), expect);
            }
        };
    }

    macro_rules! macro_test_into_inner {
        ($name:ident, $type:ty, $($value:expr),*) => {
            #[rstest]
            $(
                #[case($value)]
            )*
            fn $name(#[case] value:$type){
                assert!(value.into_inner().is_none());
            }
        };
        ($name:ident, $type:ty, $type_inner:ty, $($value:expr => $expect:expr),*) => {
            #[rstest]
            $(
                #[case($value, $expect)]
            )*
            fn $name(#[case] value:$type, #[case] expect:$type_inner){
                assert_eq!(value.into_inner().unwrap(), expect);
            }
        };
    }

    macro_rules! macro_test_elapsed {
        ($name:ident, $type:ty, $($value:expr),*) => {
            #[rstest]
            $(
                #[case($value)]
            )*
            fn $name(#[case] value:$type){
                assert!(value.elapsed().is_none());
            }
        };
        ($name:ident, $type:ty, $type_elapsed:ty, $($value:expr => $expect:expr),*) => {
            #[rstest]
            $(
                #[case($value, $expect)]
            )*
            fn $name(#[case] value:$type, #[case] expect:$type_elapsed){
                assert_eq!(value.elapsed().unwrap(), expect);
            }
        };
    }

    macro_rules! macro_test_scale_mantissa {
        ($name:ident, $type:ty, $($value:expr),*) => {
            #[rstest]
            $(
                #[case($value)]
            )*
            fn $name(#[case] value:$type){
                assert!(value.scale().is_none());
                assert!(value.mantissa().is_none());
            }
        };
        ($name:ident, $type:ty, $type_mantissa:ty, $($value:expr => ($expect_mantissa:expr, $expect_scale:expr)),*) => {
            #[rstest]
            $(
                #[case($value, $expect_mantissa, $expect_scale)]
            )*
            fn $name(#[case] value:$type, #[case] expect_mantissa:$type_mantissa, #[case] expect_scale:u32){
                assert_eq!(value.scale().unwrap(), expect_scale);
                assert_eq!(value.mantissa().unwrap(), expect_mantissa);
            }
        };
    }

    macro_rules! macro_test_rescale {
        ($name:ident, $type:ty, $($value:expr => ($rescale:expr, $expect:expr)),*) => {
            #[rstest]
            $(
                #[case($value, $rescale, $expect)]
            )*
            fn $name(#[case] mut value:$type, #[case] rescale:u32, #[case] expect:$type){
                value.rescale(rescale);
                assert_eq!(value, expect);
            }
        };
        ($name:ident, $type:ty, $($value:expr => $rescale:expr),*) => {
            #[rstest]
            $(
                #[case($value, $rescale)]
            )*
            fn $name(#[case] mut value:$type, #[case] rescale:u32){
                value.rescale(rescale);
                assert!(value.is_null());
            }
        };
    }

    // is_null
    macro_test_is_null!(
        test_types_func_type_is_null_void,
        Void,
        Void::from(Option::None) => true,
        Void::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_bool,
        Bool,
        Bool::new(true) => false,
        Bool::new(false) => false,
        Bool::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_char,
        Char,
        Char::new(0i8) => false,
        Char::from(Option::None) => true,
        Char::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_short,
        Short,
        Short::new(0i16) => false,
        Short::from(Option::None) => true,
        Short::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_int,
        Int,
        Int::new(0i32) => false,
        Int::from(Option::None) => true,
        Int::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_long,
        Long,
        Long::new(0i64) => false,
        Long::from(Option::None) => true,
        Long::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_date,
        Date,
        Date::from_ymd(1970, 1, 1).unwrap() => false,
        Date::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_month,
        Month,
        Month::from_ym(1970, 1).unwrap() => false,
        Month::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_time,
        Time,
        Time::from_hms_milli(0, 0, 0, 0).unwrap() => false,
        Time::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_minute,
        Minute,
        Minute::from_hm(0, 0).unwrap() => false,
        Minute::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_second,
        Second,
        Second::from_hms(0, 0, 0).unwrap() => false,
        Second::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_datetime,
        DateTime,
        DateTime::from_raw(0i32).unwrap() => false,
        DateTime::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_timestamp,
        Timestamp,
        Timestamp::from_raw(0i64).unwrap() => false,
        Timestamp::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_nanotime,
        NanoTime,
        NanoTime::from_hms_nano(0, 0, 0, 0).unwrap() => false,
        NanoTime::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_nanotimestamp,
        NanoTimestamp,
        NanoTimestamp::from_raw(0).unwrap() => false,
        NanoTimestamp::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_datehour,
        DateHour,
        DateHour::from_ymd_h(1970, 1, 1, 0).unwrap() => false,
        DateHour::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_float,
        Float,
        Float::new(0.0f32) => false,
        Float::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_double,
        Double,
        Double::new(0.0f64) => false,
        Double::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_string,
        DolphinString,
        DolphinString::new(String::from("abc!@#中文 123")) => false,
        DolphinString::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_blob,
        Blob,
        Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()) => false,
        Blob::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_decimal32,
        Decimal32,
        Decimal32::from_raw(0i32, 3).unwrap() => false,
        Decimal32::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_decimal64,
        Decimal64,
        Decimal64::from_raw(0i64, 3).unwrap() => false,
        Decimal64::default() => true
    );
    macro_test_is_null!(
        test_types_func_type_is_null_decimal128,
        Decimal128,
        Decimal128::from_raw(0i128, 3).unwrap() => false,
        Decimal128::default() => true
    );

    // into_inner
    macro_test_into_inner!(
        test_types_func_type_into_inner_void,
        Void,
        Void::from(Option::None),
        Void::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_bool,
        Bool,
        bool,
        Bool::new(true) => true,
        Bool::new(false) => false
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_bool_none,
        Bool,
        Bool::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_char,
        Char,
        i8,
        Char::new(0i8) => 0i8
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_char_none,
        Char,
        Char::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_short,
        Short,
        i16,
        Short::new(0i16) => 0i16
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_short_none,
        Short,
        Short::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_int,
        Int,
        i32,
        Int::new(0i32) => 0i32
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_int_none,
        Int,
        Int::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_long,
        Long,
        i64,
        Long::new(0i64) => 0i64
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_long_none,
        Long,
        Long::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_date,
        Date,
        NaiveDate,
        Date::from_ymd(1970, 1, 1).unwrap() => NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        Date::from_raw(1).unwrap() => NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_date_none,
        Date,
        Date::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_month,
        Month,
        NaiveDate,
        Month::from_ym(1970, 1).unwrap() => NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        Month::from_raw(1).unwrap() => NaiveDate::from_ymd_opt(1970, 2, 1).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_month_none,
        Month,
        Month::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_time,
        Time,
        NaiveTime,
        Time::from_hms_milli(0, 0, 0, 0).unwrap() => NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
        Time::from_raw(1).unwrap() => NaiveTime::from_hms_milli_opt(0, 0, 0, 1).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_time_none,
        Time,
        Time::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_minute,
        Minute,
        NaiveTime,
        Minute::from_hm(0, 0).unwrap() => NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
        Minute::from_raw(1).unwrap() => NaiveTime::from_hms_milli_opt(0, 1, 0, 0).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_minute_none,
        Minute,
        Minute::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_second,
        Second,
        NaiveTime,
        Second::from_hms(0, 0, 0).unwrap() => NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
        Second::from_raw(1).unwrap() => NaiveTime::from_hms_milli_opt(0, 0, 1, 0).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_second_none,
        Second,
        Second::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_datetime,
        DateTime,
        NaiveDateTime,
        DateTime::from_date_second(
            Date::from_ymd(1970, 1, 1).unwrap(),
            Second::from_hms(0, 0, 0).unwrap()
        ).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap()
        ),
        DateTime::from_raw(1).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_milli_opt(0, 0, 1, 0).unwrap()
        )
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_datetime_none,
        DateTime,
        DateTime::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_timestamp,
        Timestamp,
        NaiveDateTime,
        Timestamp::from_date_time(
            Date::from_ymd(1970, 1, 1).unwrap(),
            Time::from_hms_milli(0, 0, 0, 0).unwrap()
        ).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap()
        ),
        Timestamp::from_raw(1i64).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_milli_opt(0, 0, 0, 1).unwrap()
        )
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_timestamp_none,
        Timestamp,
        Timestamp::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_nanotime,
        NanoTime,
        NaiveTime,
        NanoTime::from_hms_nano(0, 0, 0, 0).unwrap() => NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap(),
        NanoTime::from_raw(1u64).unwrap() => NaiveTime::from_hms_nano_opt(0, 0, 0, 1).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_nanotime_none,
        NanoTime,
        NanoTime::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_nanotimestamp,
        NanoTimestamp,
        NaiveDateTime,
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(1970, 1, 1).unwrap(),
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
        ).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap()
        ),
        NanoTimestamp::from_raw(1i64).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_nano_opt(0, 0, 0, 1).unwrap()
        )
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_nanotimestamp_none,
        NanoTimestamp,
        NanoTimestamp::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_datehour,
        DateHour,
        NaiveDateTime,
        DateHour::from_ymd_h(1970, 1, 1, 0).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap()
        ),
        DateHour::from_raw(1i64).unwrap() => NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970,1,1).unwrap(),
            NaiveTime::from_hms_nano_opt(1, 0, 0, 0).unwrap()
        )
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_datehour_none,
        DateHour,
        DateHour::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_float,
        Float,
        f32,
        Float::new(0.0f32) => 0.0f32//,
        // Float::new(f32::NAN) => f32::NAN
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_float_none,
        Float,
        Float::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_double,
        Double,
        f64,
        Double::new(0.0f64) => 0.0f64//,
        // Float::new(f32::NAN) => f32::NAN
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_double_none,
        Double,
        Double::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_string,
        DolphinString,
        String,
        DolphinString::new(String::from("abc!@#中文 123")) => String::from("abc!@#中文 123")
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_string_none,
        DolphinString,
        DolphinString::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_blob,
        Blob,
        Vec::<u8>,
        Blob::new(GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()) => GBK.encode("abc!@#中文 123", EncoderTrap::Strict).unwrap()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_blob_none,
        Blob,
        Blob::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_decimal32,
        Decimal32,
        Decimal,
        Decimal32::from_raw(0i32, 3).unwrap() => Decimal::new(0, 3),
        Decimal32::from_raw(314i32, 3).unwrap() => Decimal::new(314, 3)
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_decimal32_none,
        Decimal32,
        Decimal32::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_decimal64,
        Decimal64,
        Decimal,
        Decimal64::from_raw(0i64, 6).unwrap() => Decimal::new(0, 6),
        Decimal64::from_raw(314i64, 6).unwrap() => Decimal::new(314, 6)
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_decimal64_none,
        Decimal64,
        Decimal64::default()
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_decimal128,
        Decimal128,
        Decimal,
        Decimal128::from_raw(0i128, 26).unwrap() => Decimal::new(0, 26),
        Decimal128::from_raw(314i128, 26).unwrap() => Decimal::new(314, 26)
    );
    macro_test_into_inner!(
        test_types_func_type_into_inner_decimal128_none,
        Decimal128,
        Decimal128::default()
    );

    // from_date_second from_date_time from_date_nanotime
    #[test]
    fn test_types_func_type_datetime_from_date_second_none() {
        let result_1 = DateTime::from_date_second(Date::default(), Second::default());
        assert!(result_1.is_none());
        let result_2 =
            DateTime::from_date_second(Date::from_ymd(1970, 1, 1).unwrap(), Second::default());
        assert!(result_2.is_none());
    }

    #[test]
    fn test_types_func_type_timestamp_from_date_time_none() {
        let result_1 = Timestamp::from_date_time(Date::default(), Time::default());
        assert!(result_1.is_none());
        let result_2 =
            Timestamp::from_date_time(Date::from_ymd(1970, 1, 1).unwrap(), Time::default());
        assert!(result_2.is_none());
    }

    #[test]
    fn test_types_func_type_nanotimestamp_from_date_nanotime_none() {
        let result_1 = NanoTimestamp::from_date_nanotime(Date::default(), NanoTime::default());
        assert!(result_1.is_none());
        let result_2 = NanoTimestamp::from_date_nanotime(
            Date::from_ymd(1970, 1, 1).unwrap(),
            NanoTime::default(),
        );
        assert!(result_2.is_none());
    }

    // from_raw
    #[test]
    fn test_types_func_type_date_from_raw_elapsed_lt_0() {
        let result = Date::from_raw(-1).unwrap();
        assert_eq!(result, Date::from_ymd(1969, 12, 31).unwrap());
    }

    #[test]
    fn test_types_func_type_decimal32_from_raw_scale_gt_9() {
        let result = Decimal32::from_raw(314, 10);
        assert!(result.is_none());
    }

    #[test]
    fn test_types_func_type_decimal64_from_raw_scale_gt_18() {
        let result = Decimal64::from_raw(314, 19);
        assert!(result.is_none());
    }

    // as mut
    #[test]
    fn test_types_func_type_float_as_mut() {
        let result = Float::new(3.14f32).as_mut().unwrap();
        assert_eq!(result, 3.14f32);
    }

    // hash
    #[test]
    fn test_types_func_type_float_hash() {
        let mut hasher = DefaultHasher::new();
        Float::new(f32::NAN).hash(&mut hasher);
    }

    // ord
    #[test]
    fn test_types_func_type_float_partial_ord_cmp() {
        let x = Float::new(3.14);
        let y = Float::new(3.15);
        assert_eq!(x.cmp(&y), Ordering::Less);
        assert_eq!(y.cmp(&x), Ordering::Greater);
        assert_eq!(x.cmp(&x), Ordering::Equal);
    }

    #[test]
    fn test_types_func_type_float_partial_ord_partial_cmp() {
        let x = Float::new(3.14);
        let y = Float::new(3.15);
        let result = x.partial_cmp(&y).unwrap();
        assert_eq!(result, Ordering::Less);
    }

    #[test]
    fn test_types_func_type_float_partial_ord_1() {
        let x = Float::new(3.14);
        let y = Float::new(3.15);
        assert!(!(x > y));
        assert!(!(x >= y));
        assert!(x < y);
        assert!(x <= y);
        assert!(!(x == y));
        assert!(x != y);
    }

    #[test]
    fn test_types_func_type_float_partial_ord_2() {
        let x = Float::new(3.14);
        let y = Float::default();
        assert!(x > y);
        assert!(x >= y);
        assert!(!(x < y));
        assert!(!(x <= y));
        assert!(!(x == y));
        assert!(x != y);
    }

    #[test]
    fn test_types_func_type_float_partial_ord_3() {
        let x = Float::default();
        let y = Float::default();
        assert!(!(x > y));
        assert!(x >= y);
        assert!(!(x < y));
        assert!(x <= y);
        assert!(x == y);
        assert!(!(x != y));
    }

    // new
    #[test]
    fn test_types_func_type_void_new() {
        let _result = Void::new(()).to_owned();
    }

    // try_from
    #[test]
    fn test_types_func_type_try_from_date() {
        let result_1 = NaiveDate::try_from(Date::default());
        assert!(result_1.is_err());
        let result_2 = NaiveDate::try_from(Date::from_ymd(1970, 1, 1).unwrap());
        assert_eq!(
            result_2.unwrap(),
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        );
    }

    #[test]
    fn test_types_func_type_try_from_any() {
        let result: Any = Bool::new(true).into();
        assert_eq!(Bool::try_from(result).unwrap(), Bool::new(true));
    }

    #[test]
    fn test_types_func_type_try_from_float() {
        let result = f32::try_from(Float::new(3.14f32)).unwrap();
        assert_eq!(result, 3.14f32);
        let result = f32::try_from(Float::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_types_func_type_try_from_scalar() {
        let result: ConstantImpl = Decimal32::default().into();
        assert_eq!(Decimal32::try_from(result).unwrap(), Decimal32::default());
    }

    #[test]
    fn test_types_func_type_try_from_decimal32() {
        assert!(Decimal::try_from(Decimal32::default()).is_err());
        assert_eq!(
            Decimal::try_from(Decimal32::from_raw(314, 2).unwrap()).unwrap(),
            Decimal::from_str("3.14").unwrap()
        );
    }

    #[test]
    fn test_types_func_type_try_from_decimal64() {
        assert!(Decimal::try_from(Decimal64::default()).is_err());
        assert_eq!(
            Decimal::try_from(Decimal64::from_raw(314, 2).unwrap()).unwrap(),
            Decimal::from_str("3.14").unwrap()
        );
    }

    #[test]
    fn test_types_func_type_try_from_decimal128() {
        assert!(Decimal::try_from(Decimal128::default()).is_err());
        assert_eq!(
            Decimal::try_from(Decimal128::from_raw(314, 2).unwrap()).unwrap(),
            Decimal::from_str("3.14").unwrap()
        );
    }

    // elapsed
    macro_test_elapsed!(
        test_types_func_type_elapsed_date,
        Date,
        i64,
        Date::from_ymd(1970, 1, 1).unwrap() => 0i64,
        Date::from_ymd(2022, 5, 20).unwrap() => 19132i64
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_date_null,
        Date,
        Date::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_month,
        Month,
        i32,
        Month::from_ym(1970, 1).unwrap() => 0i32,
        Month::from_ym(2022, 5).unwrap() => 628i32
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_month_null,
        Month,
        Month::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_time,
        Time,
        u32,
        Time::from_hms_milli(0, 0, 0, 0).unwrap() => 0u32,
        Time::from_hms_milli(13, 50, 59, 123).unwrap() => 49859123u32
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_time_null,
        Time,
        Time::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_minute,
        Minute,
        u32,
        Minute::from_hm(0, 0).unwrap() => 0u32,
        Minute::from_hm(13, 50).unwrap() => 830u32
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_minute_null,
        Minute,
        Minute::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_second,
        Second,
        u32,
        Second::from_hms(0, 0, 0).unwrap() => 0u32,
        Second::from_hms(13, 50, 59).unwrap() => 49859u32
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_second_null,
        Second,
        Second::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_datetime,
        DateTime,
        i32,
        DateTime::from_date_second(
            Date::from_ymd(1970, 1, 1).unwrap(),
            Second::from_hms(0, 0, 0).unwrap()
        ).unwrap() => 0i32,
        DateTime::from_date_second(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Second::from_hms(13, 50, 59).unwrap()
        ).unwrap() => 1653054659i32
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_datetime_null,
        DateTime,
        DateTime::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_timestamp,
        Timestamp,
        i64,
        Timestamp::from_raw(0i64).unwrap() => 0i64,
        Timestamp::from_date_time(
            Date::from_ymd(2022, 5, 20).unwrap(),
            Time::from_hms_milli(13, 50, 59, 123).unwrap()
        ).unwrap() => 1653054659123i64
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_timestamp_null,
        Timestamp,
        Timestamp::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_nanotime,
        NanoTime,
        u64,
        NanoTime::from_hms_nano(0, 0, 0, 0).unwrap() => 0u64,
        NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap() => 49859123456789u64
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_nanotime_null,
        NanoTime,
        NanoTime::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_nanotimestamp,
        NanoTimestamp,
        i64,
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(1970, 1, 1).unwrap(),
            NanoTime::from_hms_nano(0, 0, 0, 0).unwrap()
        ).unwrap() => 0i64,
        NanoTimestamp::from_date_nanotime(
            Date::from_ymd(2022, 5, 20).unwrap(),
            NanoTime::from_hms_nano(13, 50, 59, 123456789).unwrap()
        ).unwrap() => 1653054659123456789i64
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_nanotimestamp_null,
        NanoTimestamp,
        NanoTimestamp::default()
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_datehour,
        DateHour,
        i64,
        DateHour::from_ymd_h(1970, 1, 1, 0).unwrap() => 0i64,
        DateHour::from_ymd_h(2022, 5, 20, 13).unwrap() => 459181i64
    );
    macro_test_elapsed!(
        test_types_func_type_elapsed_datehour_null,
        DateHour,
        DateHour::default()
    );

    // scale & mantissa
    macro_test_scale_mantissa!(
        test_types_func_type_scale_mantissa_decimal32,
        Decimal32,
        i32,
        Decimal32::from_raw(0i32, 3).unwrap() => (0i32, 3u32),
        Decimal32::from_raw(314i32, 3).unwrap() => (314i32, 3u32)
    );
    macro_test_scale_mantissa!(
        test_types_func_type_scale_mantissa_decimal32_null,
        Decimal32,
        Decimal32::default()
    );
    macro_test_scale_mantissa!(
        test_types_func_type_scale_mantissa_decimal64,
        Decimal64,
        i64,
        Decimal64::from_raw(0i64, 3).unwrap() => (0i64, 3u32),
        Decimal64::from_raw(314i64, 3).unwrap() => (314i64, 3u32)
    );
    macro_test_scale_mantissa!(
        test_types_func_type_scale_mantissa_decimal64_null,
        Decimal64,
        Decimal64::default()
    );
    macro_test_scale_mantissa!(
        test_types_func_type_scale_mantissa_decimal128,
        Decimal128,
        i128,
        Decimal128::from_raw(0i128, 3).unwrap() => (0i128, 3u32),
        Decimal128::from_raw(314i128, 3).unwrap() => (314i128, 3u32)
    );
    macro_test_scale_mantissa!(
        test_types_func_type_scale_mantissa_decimal128_null,
        Decimal128,
        Decimal128::default()
    );

    // rescale
    macro_test_rescale!(
        test_types_func_type_rescale_decimal32,
        Decimal32,
        Decimal32::from_raw(3141592i32, 6).unwrap() => (3u32, Decimal32::from_raw(3142i32, 3).unwrap()),
        Decimal32::from_raw(3141592i32, 6).unwrap() => (8u32, Decimal32::from_raw(314159200i32, 8).unwrap())
    );
    macro_test_rescale!(
        test_types_func_type_rescale_decimal32_null,
        Decimal32,
        Decimal32::default() => 3u32
    );
    macro_test_rescale!(
        test_types_func_type_rescale_decimal64,
        Decimal64,
        Decimal64::from_raw(3141592i64, 6).unwrap() => (3u32, Decimal64::from_raw(3142i64, 3).unwrap()),
        Decimal64::from_raw(3141592i64, 6).unwrap() => (8u32, Decimal64::from_raw(314159200i64, 8).unwrap())
    );
    macro_test_rescale!(
        test_types_func_type_rescale_decimal64_null,
        Decimal64,
        Decimal64::default() => 3u32
    );
    macro_test_rescale!(
        test_types_func_type_rescale_decimal128,
        Decimal128,
        Decimal128::from_raw(3141592i128, 6).unwrap() => (3u32, Decimal128::from_raw(3142i128, 3).unwrap()),
        Decimal128::from_raw(3141592i128, 6).unwrap() => (8u32, Decimal128::from_raw(314159200i128, 8).unwrap())
    );
    macro_test_rescale!(
        test_types_func_type_rescale_decimal128_null,
        Decimal128,
        Decimal128::default() => 3u32
    );

    // any
    #[test]
    fn test_types_func_type_raw_data_type_any() {
        let x: Any = Bool::new(true).into();
        assert_eq!(Any::data_type(), DataType::Any);
        assert_eq!(x.raw_data_type(), DataType::Bool);
    }

    #[test]
    fn test_types_func_type_set_get_any() {
        let mut x = Any::try_from(Bool::new(true)).unwrap();
        assert_eq!(
            x.get(),
            &ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(true)))
        );
        x.set(Bool::new(false).into());
        assert_eq!(
            x.get(),
            &ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(false)))
        );
        assert_eq!(
            x.get_mut(),
            &ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(false)))
        );
    }

    macro_test_is_null!(
        test_types_func_type_is_null_any,
        Any,
        Any::new(Bool::new(true).into()) => false,
        Any::new(Bool::default().into()) => true,
        Any::default() => true
    );

    #[test]
    fn test_types_func_type_into_inner_any() {
        let any_normal =
            Any::new(ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(true)))).into_inner();
        assert_eq!(
            any_normal,
            ConstantImpl::Scalar(ScalarImpl::Bool(Bool::new(true)))
        );
        let any_default = Any::default().into_inner();
        assert_eq!(any_default, ConstantImpl::Scalar(ScalarImpl::default()));
        assert_eq!(any_default, ConstantImpl::default());
    }
}

mod test_types_func_form {
    use super::*;

    // constant impl
    #[test]
    fn test_types_func_form_constant_impl_is_null() {
        let constant_impl_1: ConstantImpl = Bool::new(true).into();
        assert!(!constant_impl_1.is_null());
        let constant_impl_2: ConstantImpl =
            PairImpl::Bool(Pair::<Bool>::new((Bool::new(true), Bool::new(false)))).into();
        assert!(!constant_impl_2.is_null());
    }

    #[test]
    fn test_types_func_form_constant_impl_get() {
        let constant_impl: ConstantImpl = Bool::new(true).into();
        assert!(constant_impl.get(0).is_none());
    }

    // pair
    #[test]
    fn test_types_func_form_pair_first_second() {
        let mut pair = Pair::<Bool>::new((Bool::new(true), Bool::new(false)));
        assert_eq!(*pair.first(), Bool::new(true));
        assert_eq!(*pair.second(), Bool::new(false));
        assert_eq!(*pair.first_mut(), Bool::new(true));
        assert_eq!(*pair.second_mut(), Bool::new(false));
    }

    // pair impl
    #[test]
    fn test_types_func_form_pair_impl_try_from_error() {
        let result = PairImpl::try_from(VectorImpl::Blob(Vector::<Blob>::new()));
        assert!(result.is_err());
    }

    #[test]
    fn test_types_func_form_pair_impl_from_error() {
        let result: PairImpl = Pair::<Bool>::new((Bool::new(true), Bool::new(false))).into();
        let expect = PairImpl::Bool(Pair::<Bool>::new((Bool::new(true), Bool::new(false))));
        assert_eq!(result, expect);
    }

    // vector
    #[test]
    fn test_types_func_form_vector_from_vec_scalar_impl() {
        let vector =
            Vector::<Bool>::try_from(VectorImpl::Bool(vector_build!(Bool, Bool::new(true))))
                .unwrap();
        let result: Vec<ScalarImpl> = vector.into();
        assert_eq!(result[0], Bool::new(true).into());
    }

    #[test]
    fn test_types_func_form_vector_index_mut() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        vector[0] = Bool::new(false);
        assert_eq!(vector[0], Bool::new(false));
    }

    #[test]
    fn test_types_func_form_vector_clear_len_is_empty() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        assert!(!vector.is_empty());
        assert_eq!(vector.len(), 3);
        vector.clear();
        assert!(vector.is_empty());
        assert_eq!(vector.len(), 0);
    }

    #[test]
    fn test_types_func_form_vector_first_last() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        assert_eq!(*vector.first().unwrap(), Bool::new(true));
        assert_eq!(*vector.first_mut().unwrap(), Bool::new(true));
        assert_eq!(*vector.last().unwrap(), Bool::default());
        assert_eq!(*vector.last_mut().unwrap(), Bool::default());
        vector.clear();
        assert!(vector.first().is_none());
        assert!(vector.first_mut().is_none());
        assert!(vector.last().is_none());
        assert!(vector.last_mut().is_none());
    }

    #[test]
    fn test_types_func_form_vector_pop() {
        let mut vector = vector_build!(Bool, Bool::new(true));
        assert_eq!(vector.len(), 1);
        assert_eq!(vector.pop().unwrap(), Bool::new(true));
        assert_eq!(vector.len(), 0);
        assert!(vector.pop().is_none());
    }

    #[test]
    fn test_types_func_form_vector_append() {
        let mut vector = vector_build!(Bool, Bool::new(true));
        vector.append(&mut vector_build!(Bool, Bool::new(false)));
        assert_eq!(
            vector,
            vector_build!(Bool, Bool::new(true), Bool::new(false))
        )
    }

    #[test]
    fn test_types_func_form_vector_remove() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        vector.remove(0);
        assert_eq!(
            vector,
            vector_build!(Bool, Bool::new(false), Bool::default())
        );
    }

    #[test]
    fn test_types_func_form_vector_swap_remove() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        vector.swap_remove(0);
        assert_eq!(
            vector,
            vector_build!(Bool, Bool::default(), Bool::new(false))
        );
    }

    #[test]
    fn test_types_func_form_vector_truncate() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        vector.truncate(3);
        assert_eq!(
            vector,
            vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default())
        );
        vector.truncate(1);
        assert_eq!(vector, vector_build!(Bool, Bool::new(true)));
    }

    #[test]
    fn test_types_func_form_vector_iter() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        let _vector = vector_build!(Bool, Bool::new(true), Bool::new(false), Bool::default());
        let mut index = 0;
        for i in vector.iter() {
            assert_eq!(*i, _vector[index]);
            index += 1;
        }
        let mut index = 0;
        for i in vector.iter_mut() {
            assert_eq!(*i, _vector[index]);
            index += 1;
        }
    }

    #[test]
    fn test_types_func_form_vector_resize() {
        let mut vector = vector_build!(Bool, Bool::new(true), Bool::new(false));
        vector.resize(5, Bool::default());
        assert_eq!(
            vector,
            vector_build!(
                Bool,
                Bool::new(true),
                Bool::new(false),
                Bool::default(),
                Bool::default(),
                Bool::default()
            )
        );
    }

    #[test]
    fn test_types_func_form_vector_primitive() {
        let mut vector = Vector::<DolphinString>::from_raw(&vec!["1", "2", "3"]);
        assert_eq!(
            vector,
            vector_build!(
                DolphinString,
                DolphinString::new("1".into()),
                DolphinString::new("2".into()),
                DolphinString::new("3".into())
            )
        );
        vector.push_raw("4");
        assert_eq!(
            vector,
            vector_build!(
                DolphinString,
                DolphinString::new("1".into()),
                DolphinString::new("2".into()),
                DolphinString::new("3".into()),
                DolphinString::new("4".into())
            )
        );
    }

    #[test]
    fn test_types_func_form_vector_any() {
        let mut vector = vector_build!(
            Any,
            Any::new(Int::new(1i32).into()),
            Any::new(Int::new(2i32).into()),
            Any::new(Int::new(3i32).into())
        );
        vector.push_raw(Int::new(4i32).into());
        assert_eq!(vector.data_type(), DataType::Any);
        assert_eq!(
            vector,
            vector_build!(
                Any,
                Any::new(Int::new(1i32).into()),
                Any::new(Int::new(2i32).into()),
                Any::new(Int::new(3i32).into()),
                Any::new(Int::new(4i32).into())
            )
        )
    }

    // vector impl
    #[test]
    fn test_types_func_form_vector_impl_is_empty_push_saclar() {
        let mut vector_impl = VectorImpl::Bool(Vector::<Bool>::new());
        assert!(vector_impl.is_empty());
        vector_impl.push_scalar(Bool::new(true).into());
        assert!(!vector_impl.is_empty());
    }

    #[test]
    fn test_types_func_form_vector_impl_push() {
        let mut vector_impl = VectorImpl::Bool(Vector::<Bool>::new());
        let result_1 = vector_impl.push(Bool::new(true).into());
        assert!(result_1.is_ok());
        let result_2 = vector_impl.push(Char::new(0i8).into());
        assert!(result_2.is_err());
    }

    #[test]
    fn test_types_func_form_vector_impl_push_error() {
        let mut vector_impl = VectorImpl::Bool(Vector::<Bool>::new());
        let result_1 = vector_impl.push(vector_build!(Bool, Bool::new(true)).into());
        assert!(result_1.is_err());
    }

    #[test]
    fn test_types_func_form_vector_impl_push_any() {
        let mut vector_impl = VectorImpl::Any(Vector::<Any>::new());
        let result_1 = vector_impl
            .push(VectorImpl::Any(vector_build!(Any, Any::new(Bool::new(true).into()))).into());
        assert!(result_1.is_ok());
    }

    #[test]
    fn test_types_func_form_vector_impl_push_unchecked() {
        let mut vector_impl = VectorImpl::Bool(Vector::<Bool>::new());
        vector_impl.push_unchecked(Bool::new(true).into());
        assert_eq!(vector_impl.len(), 1);
    }

    #[test]
    #[should_panic]
    fn test_types_func_form_vector_impl_push_unchecked_panic() {
        let mut vector_impl = VectorImpl::Bool(Vector::<Bool>::new());
        let _ = vector_impl.push_unchecked(vector_build!(Bool, Bool::new(true)).into());
    }

    #[test]
    fn test_types_func_form_vector_impl_push_unchecked_any() {
        let mut vector_impl = VectorImpl::Any(Vector::<Any>::new());
        vector_impl.push_unchecked(
            VectorImpl::Any(vector_build!(Any, Any::new(Bool::new(true).into()))).into(),
        );
        assert_eq!(vector_impl.len(), 1);
    }

    #[test]
    fn test_types_func_form_vector_impl_break_up() {
        let vector_impl = VectorImpl::Bool(vector_build!(Bool, Bool::new(true)));
        let _ = vector_impl.break_up();
    }

    #[test]
    fn test_types_func_form_vector_impl_get() {
        let vector_impl = VectorImpl::Bool(vector_build!(Bool, Bool::new(true)));
        let result = vector_impl.get(0).unwrap();
        assert_eq!(result, Bool::new(true).into());
    }

    // array vector
    #[test]
    fn test_types_func_form_array_vector_clear_len_is_empty() {
        let mut array_vector =
            array_vector_build!(i32, vec![0i32, 1i32, 2i32], vec![0i32], vec![1i32]);
        assert!(!array_vector.is_empty());
        assert_eq!(array_vector.len(), 3);
        array_vector.clear();
        assert!(array_vector.is_empty());
        assert_eq!(array_vector.len(), 0);
    }

    #[test]
    fn test_types_func_form_array_vector_clear_len_eq() {
        let array_vector_1 =
            array_vector_build!(i32, vec![0i32, 1i32, 2i32], vec![0i32], vec![1i32]);
        let mut array_vector_2 =
            array_vector_build!(i32, vec![0i32, 1i32, 2i32], vec![0i32], vec![1i32]);
        assert_eq!(array_vector_1, array_vector_2);
        array_vector_2.clear();
        assert_ne!(array_vector_1, array_vector_2);
    }

    #[test]
    fn test_types_func_form_array_vector_display() {
        let array_vector = array_vector_build!(i32, vec![0i32, 1i32, 2i32], vec![], vec![-127i32]);
        println!("{array_vector}");
    }

    #[test]
    fn test_types_func_form_array_vector_from() {
        let _vector_impl: VectorImpl =
            array_vector_build!(i32, vec![0i32, 1i32, 2i32], vec![], vec![-127i32]).into();
        let _constant_impl: ConstantImpl =
            array_vector_build!(i32, vec![0i32, 1i32, 2i32], vec![], vec![-127i32]).into();
    }

    // array vector impl
    #[test]
    fn test_types_func_form_array_vector_impl_len_is_empty() {
        let array_vector_impl_1 = ArrayVectorImpl::Int(array_vector_build!(
            i32,
            vec![0i32, 1i32, 2i32],
            vec![0i32],
            vec![1i32]
        ));
        assert!(!array_vector_impl_1.is_empty());
        assert_eq!(array_vector_impl_1.len(), 3);
        let array_vector_impl_2 = ArrayVectorImpl::Int(ArrayVector::<i32>::new());
        assert!(array_vector_impl_2.is_empty());
        assert_eq!(array_vector_impl_2.len(), 0);
    }

    #[test]
    fn test_types_func_form_array_vector_impl_data_form_data_type() {
        let array_vector_impl = ArrayVectorImpl::Int(ArrayVector::<i32>::new());
        assert_eq!(array_vector_impl.data_form(), DataForm::Vector);
        assert_eq!(array_vector_impl.data_type(), DataType::IntArray);
    }

    #[test]
    fn test_types_func_form_array_vector_impl_display() {
        let array_vector_impl = ArrayVectorImpl::Int(array_vector_build!(
            i32,
            vec![0i32, 1i32, 2i32],
            vec![],
            vec![-127i32]
        ));
        println!("{array_vector_impl}");
    }

    // set
    #[test]
    fn test_types_func_form_set_capacity_reserve_shrink_to_fit() {
        let mut result = Set::<Char>::with_capacity(3);
        assert_eq!(result.capacity(), 3);
        result.reserve(2);
        assert_eq!(result.capacity(), 3);
        result.reserve(5);
        assert!(result.capacity() >= 5);
        result.shrink_to_fit();
        assert_eq!(result.capacity(), 0);
    }

    #[test]
    fn test_types_func_form_set_iter() {
        let result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        let mut expect = Set::<Int>::with_capacity(3);
        for i in result.iter() {
            expect.insert(*i);
        }
        assert_eq!(result, expect);
    }

    #[test]
    fn test_types_func_form_set_len_is_empty_clear() {
        let mut result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 3);
        result.clear();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_types_func_form_set_contain() {
        let result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        assert!(!result.contains(&Int::new(0i32)));
        assert!(result.contains(&Int::new(1i32)));
    }

    #[test]
    fn test_types_func_form_set_get() {
        let result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        assert!(result.get(&Int::new(0i32)).is_none());
        assert_eq!(*result.get(&Int::new(1i32)).unwrap(), Int::new(1i32));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_types_func_form_set_take() {
        let mut result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        assert!(result.take(&Int::new(0i32)).is_none());
        assert_eq!(result.take(&Int::new(1i32)).unwrap(), Int::new(1i32));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_types_func_form_set_insert() {
        let mut result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        assert!(result.insert(Int::new(0i32)));
        assert_eq!(result.len(), 4);
        assert!(!result.insert(Int::new(0i32)));
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_types_func_form_set_remove() {
        let mut result = set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32));
        assert!(result.remove(&Int::new(1i32)));
        assert_eq!(result.len(), 2);
        assert!(!result.remove(&Int::new(1i32)));
        assert_eq!(result.len(), 2);
    }

    // set impl
    #[test]
    fn test_types_func_form_set_impl_from() {
        let result: SetImpl =
            set_build!(Int, Int::new(1i32), Int::new(2i32), Int::new(3i32)).into();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_types_func_form_set_impl_try_from() {
        let result = SetImpl::try_from(VectorImpl::Int(Vector::<Int>::new()));
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // dictionary
    #[test]
    fn test_types_func_form_dictionary_index() {
        let result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        assert_eq!(
            result[&DolphinString::new("1".into())].clone().into_inner(),
            Int::new(1i32).into()
        );
    }

    #[test]
    fn test_types_func_form_dictionary_capacity_reserve_shrink_to_fit() {
        let mut result = Dictionary::<DolphinString>::with_capacity(3);
        assert_eq!(result.capacity(), 3);
        result.reserve(2);
        assert_eq!(result.capacity(), 3);
        result.reserve(5);
        assert!(result.capacity() >= 5);
        result.shrink_to_fit();
        assert_eq!(result.capacity(), 0);
    }

    #[test]
    fn test_types_func_form_dictionary_keys() {
        let result =
            dictionary_build!(DolphinString,DolphinString::new("1".into()) => Int::new(1i32));
        let key = result.keys();
        assert_eq!(key.len(), result.len());
        for i in key {
            assert!(result.contains_key(i));
        }
    }

    #[test]
    fn test_types_func_form_dictionary_values() {
        let result =
            dictionary_build!(DolphinString,DolphinString::new("1".into()) => Int::new(1i32));
        let value = result.values();
        assert_eq!(value.len(), result.len());
    }

    #[test]
    fn test_types_func_form_dictionary_iter() {
        let result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        let mut expect = Dictionary::<DolphinString>::with_capacity(3);
        for (key, value) in result.iter() {
            expect.insert(
                key.clone(),
                *value
                    .clone()
                    .into_inner()
                    .as_scalar()
                    .unwrap()
                    .as_int()
                    .unwrap(),
            );
        }
        assert_eq!(result, expect);
    }

    #[test]
    fn test_types_func_form_dictionary_into_iter() {
        let result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        println!("{}", result[&DolphinString::new("1".into())]);
        let mut expect = Dictionary::<DolphinString>::with_capacity(3);
        for (key, value) in result.clone().into_iter() {
            expect.insert(
                key.clone(),
                *value
                    .clone()
                    .into_inner()
                    .as_scalar()
                    .unwrap()
                    .as_int()
                    .unwrap(),
            );
        }
        assert_eq!(result, expect);
    }

    #[test]
    fn test_types_func_form_dictionary_iter_mut() {
        let mut result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        let mut expect = Dictionary::<DolphinString>::with_capacity(3);
        for (key, value) in result.iter_mut() {
            expect.insert(
                key.clone(),
                *value
                    .clone()
                    .into_inner()
                    .as_scalar()
                    .unwrap()
                    .as_int()
                    .unwrap(),
            );
        }
        assert_eq!(result, expect);
    }

    #[test]
    fn test_types_func_form_dictionary_len_is_empty_clear() {
        let mut result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        assert!(!result.is_empty());
        assert_eq!(result.len(), 3);
        result.clear();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_types_func_form_dictionary_entry() {
        let mut result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        let res = result.entry(DolphinString::new("1".into()));
        assert_eq!(res.key(), &DolphinString::new("1".into()));
        assert_eq!(res.or_default(), &Any::new(Int::new(1i32).into()));
    }

    #[test]
    fn test_types_func_form_dictionary_get() {
        let mut result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        {
            let res_no = result.get(&DolphinString::new("4".into()));
            assert!(res_no.is_none());
            let res = result.get(&DolphinString::new("1".into())).unwrap();
            assert_eq!(res, &Any::new(Int::new(1i32).into()));
        }
        let res_mut_no = result.get_mut(&DolphinString::new("4".into()));
        assert!(res_mut_no.is_none());
        let res_mut = result.get_mut(&DolphinString::new("1".into())).unwrap();
        assert_eq!(res_mut, &Any::new(Int::new(1i32).into()));
    }

    #[test]
    fn test_types_func_form_dictionary_contain_key_remove() {
        let mut result = dictionary_build!(
            DolphinString,
            DolphinString::new("1".into()) => Int::new(1i32),
            DolphinString::new("2".into()) => Int::new(2i32),
            DolphinString::new("3".into()) => Int::new(3i32)
        );
        assert!(result.contains_key(&DolphinString::new("1".into())));
        assert!(!result.contains_key(&DolphinString::new("4".into())));
        assert!(result.remove(&DolphinString::new("4".into())).is_none());
        assert_eq!(
            result.remove(&DolphinString::new("1".into())).unwrap(),
            Any::new(Int::new(1i32).into())
        );
        assert!(!result.contains_key(&DolphinString::new("1".into())));
    }

    // dictionary impl
    #[test]
    fn test_types_func_form_dictionary_impl_data_type() {
        let result = DictionaryImpl::String(
            dictionary_build!(DolphinString,DolphinString::new("1".into()) => Int::new(1i32)),
        );
        assert_eq!(result.data_type(), DataType::String);
    }

    #[test]
    fn test_types_func_form_dictionary_impl_len() {
        let result: DictionaryImpl = table_build!(String::from("a") => VectorImpl::Bool(vector_build!(Bool, Bool::new(true)))).into();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_types_func_form_dictionary_impl_is_empty() {
        let result = DictionaryImpl::String(
            dictionary_build!(DolphinString,DolphinString::new("1".into()) => Int::new(1i32)),
        );
        assert!(!result.is_empty());
    }

    // table
    #[test]
    fn test_types_func_form_table_build_error_1() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        _column_name.push("a".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(2);
        _columns.push(VectorImpl::Bool(Vector::<Bool>::new()));
        _columns.push(VectorImpl::Char(Vector::<Char>::new()));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        assert!(result.build().is_err());
    }

    #[test]
    fn test_types_func_form_table_build_error_2() {
        let mut _column_name = Vec::<String>::with_capacity(2);
        _column_name.push("a".into());
        _column_name.push("b".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(2);
        _columns.push(VectorImpl::Bool(vector_build!(Bool, Bool::new(true))));
        _columns.push(VectorImpl::Char(Vector::<Char>::new()));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        assert!(result.build().is_err());
    }

    #[test]
    fn test_types_func_form_table_build_error_3() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        _column_name.push("a".into());
        _column_name.push("a".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(2);
        _columns.push(VectorImpl::Bool(Vector::<Bool>::new()));
        _columns.push(VectorImpl::Char(Vector::<Char>::new()));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        assert!(result.build().is_err());
    }

    #[test]
    fn test_types_func_form_table_build_error_4() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        _column_name.push("".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(1);
        _columns.push(VectorImpl::Bool(Vector::<Bool>::new()));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        assert!(result.build().is_err());
    }

    #[test]
    fn test_types_func_form_table_build_empty() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        let mut _columns = Vec::<VectorImpl>::with_capacity(1);
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        let table = result.build().unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_types_func_form_table_build_empty_error_1() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        let mut _columns = Vec::<VectorImpl>::with_capacity(1);
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        let mut table = result.build().unwrap();
        let x = table.insert_column(
            VectorImpl::Bool(vector_build!(Bool, Bool::new(true))),
            "a".into(),
            0,
        );
        assert!(x.is_err());
    }

    #[test]
    fn test_types_func_form_table_build_empty_error_2() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        _column_name.push("a".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(2);
        _columns.push(VectorImpl::Bool(vector_build!(Bool, Bool::new(true))));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        let mut table = result.build().unwrap();
        let x = table.insert_column(
            VectorImpl::Bool(vector_build!(Bool, Bool::new(true))),
            "a".into(),
            1,
        );
        assert!(x.is_err());
    }

    #[test]
    fn test_types_func_form_table_build_empty_error_3() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        _column_name.push("a".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(2);
        _columns.push(VectorImpl::Bool(vector_build!(Bool, Bool::new(true))));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        let mut table = result.build().unwrap();
        let x = table.insert_column(
            VectorImpl::Bool(vector_build!(Bool, Bool::new(true))),
            "".into(),
            1,
        );
        assert!(x.is_err());
    }

    #[test]
    fn test_types_func_form_table_build_empty_error_4() {
        let mut _column_name = Vec::<String>::with_capacity(1);
        _column_name.push("a".into());
        let mut _columns = Vec::<VectorImpl>::with_capacity(2);
        _columns.push(VectorImpl::Bool(vector_build!(Bool, Bool::new(true))));
        let mut result = TableBuilder::new();
        result.with_contents(_columns, _column_name);
        let mut table = result.build().unwrap();
        let x = table.insert_column(
            VectorImpl::Bool(vector_build!(Bool, Bool::new(true))),
            "b".into(),
            2,
        );
        assert!(x.is_err());
    }

    #[test]
    fn test_types_func_form_table_len_is_empty() {
        let result_1 = table_build!(String::from("a") => Vector::<Int>::new());
        assert!(result_1.is_empty());
        assert_eq!(result_1.len(), 0);
        let result_2 = table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        );
        assert!(!result_2.is_empty());
        assert_eq!(result_2.len(), 4);
    }

    #[tokio::test]
    async fn test_types_func_form_table_name() {
        let result_1 = table_build!(String::from("a") => Vector::<Int>::new());
        assert_eq!(result_1.name(), &String::from(""));
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let result_2 = client
            .run_script("x=table(1..3 as a);x")
            .await
            .unwrap()
            .unwrap();
        if let ConstantImpl::Table(x) = result_2 {
            assert_eq!(x.name(), &String::from("x"));
        } else {
            assert!(false, "error in constant");
        }
    }

    #[test]
    fn test_types_func_form_table_columns() {
        let result = table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        );
        let res = result.columns();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
            .into()
        );
    }

    #[test]
    fn test_types_func_form_table_column_names() {
        let result = table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        );
        let res = result.column_names();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], String::from("a"));
    }

    #[test]
    fn test_types_func_form_table_get_columns_by_index() {
        let result = table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        );
        let res = result.get_columns_by_index(0);
        assert_eq!(
            *res,
            vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
            .into()
        );
    }

    #[test]
    fn test_types_func_form_table_get_columns_by_name() {
        let result = table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        );
        let res = result.get_columns_by_name("a").unwrap();
        assert_eq!(
            *res,
            vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
            .into()
        );
        assert!(result.get_columns_by_name("b").is_none());
    }

    #[test]
    fn test_types_func_form_table_insert_column() {
        let mut result = table_build!(
            String::from("a") => vector_build!(
                Int,
                Int::new(2147483647i32),
                Int::new(-2147483647i32),
                Int::new(0i32),
                Int::default()
            )
        );
        let res_1 = result.insert_column(
            vector_build!(
                Long,
                Long::new(9223372036854775807i64),
                Long::new(-9223372036854775807i64),
                Long::new(0i64),
                Long::default()
            )
            .into(),
            String::from("b"),
            1,
        );
        assert!(res_1.is_ok());
        let res_2 = result.get_columns_by_name("b").unwrap();
        assert_eq!(
            *res_2,
            vector_build!(
                Long,
                Long::new(9223372036854775807i64),
                Long::new(-9223372036854775807i64),
                Long::new(0i64),
                Long::default()
            )
            .into()
        );
    }

    // data form
    #[test]
    fn test_types_func_form_data_form_try_from_error() {
        let x: Result<DataForm, _> = 7u8.try_into();
        assert!(x.is_err());
    }
}
