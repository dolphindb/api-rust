//! Temporal type interface compatible to [`chrono`]

use chrono::{
    naive::{NaiveDate, NaiveDateTime, NaiveTime},
    Datelike, Days, Months,
};

use super::super::*;
use core::fmt::Display;

impl Date {
    /// Makes a new [`Date`] from the calendar date (year, month and day).
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day).map(Self::new)
    }

    /// Makes a new [`Date`] from elapsed day since 1st of January 1970.
    pub fn from_raw(elapsed: i64) -> Option<Self> {
        if elapsed >= 0 {
            NaiveDate::default()
                .checked_add_days(Days::new(elapsed as u64))
                .map(Self::new)
        } else {
            NaiveDate::default()
                .checked_sub_days(Days::new(elapsed.unsigned_abs()))
                .map(Self::new)
        }
    }

    /// Counts of day since 1st of January 1970.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref().map(|d| (d - NaiveDate::default()).num_days())
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap() as i32
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%Y.%m.%d")),
        }
    }
}

impl Month {
    /// Makes a new [`Month`] from the calendar month (year, month).
    pub fn from_ym(year: i32, month: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, 1).map(Self::new)
    }

    /// Makes a new [`Month`] from elapsed month since January 1970.
    pub fn from_raw(elapsed: i32) -> Option<Self> {
        if elapsed >= 0 {
            NaiveDate::default()
                .checked_add_months(Months::new(elapsed as u32))
                .map(Self::new)
        } else {
            NaiveDate::default()
                .checked_sub_months(Months::new(elapsed.unsigned_abs()))
                .map(Self::new)
        }
    }

    /// Counts of month since January 1970.
    pub fn elapsed(&self) -> Option<i32> {
        self.as_ref().map(|d| {
            let start = NaiveDate::default();

            let month = d.month() as i32 - start.month() as i32;
            let year = d.year() - start.year();

            year * 12 + month
        })
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap() + 23640
    }
}

impl Display for Month {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%Y.%mM")),
        }
    }
}

impl Time {
    /// Makes a new [`Time`] from hour, minute, second and millisecond.
    pub fn from_hms_milli(hour: u32, min: u32, sec: u32, milli: u32) -> Option<Self> {
        NaiveTime::from_hms_milli_opt(hour, min, sec, milli).map(Self::new)
    }

    /// Makes a new [`Time`] from elapsed milliseconds since 00:00:00.000.
    pub fn from_raw(elapsed: u32) -> Option<Self> {
        NaiveTime::from_num_seconds_from_midnight_opt(elapsed / 1_000, elapsed % 1_000 * 1_000_000)
            .map(Self::new)
    }

    /// Counts of milliseconds since 00:00:00.000.
    pub fn elapsed(&self) -> Option<u32> {
        self.as_ref()
            .map(|t| (t - NaiveTime::default()).num_milliseconds() as u32)
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap() as i32
    }
}

impl Minute {
    /// Makes a new [`Minute`] from hour, minute.
    pub fn from_hm(hour: u32, min: u32) -> Option<Self> {
        NaiveTime::from_hms_opt(hour, min, 0).map(Self::new)
    }

    /// Makes a new [`Minute`] from elapsed minutes since 00:00.
    pub fn from_raw(elapsed: u32) -> Option<Self> {
        NaiveTime::from_num_seconds_from_midnight_opt(elapsed * 60, 0).map(Self::new)
    }

    /// Counts of minutes since 00:00.
    pub fn elapsed(&self) -> Option<u32> {
        self.as_ref()
            .map(|t| (t - NaiveTime::default()).num_minutes() as u32)
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap() as i32
    }
}

impl Second {
    /// Makes a new [`Minute`] from hour, minute and second.
    pub fn from_hms(hour: u32, min: u32, sec: u32) -> Option<Self> {
        NaiveTime::from_hms_opt(hour, min, sec).map(Self::new)
    }

    /// Makes a new [`Second`] from elapsed seconds since 00:00:00.
    pub fn from_raw(elapsed: u32) -> Option<Self> {
        NaiveTime::from_num_seconds_from_midnight_opt(elapsed, 0).map(Self::new)
    }

    /// Counts of seconds since 00:00:00.
    pub fn elapsed(&self) -> Option<u32> {
        self.as_ref()
            .map(|t| (t - NaiveTime::default()).num_seconds() as u32)
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap() as i32
    }
}

impl DateTime {
    /// Makes a new [`DateTime`] from [`Date`] and [`Second`].
    pub fn from_date_second(date: Date, second: Second) -> Option<Self> {
        if date.is_null() || second.is_null() {
            None
        } else {
            Some(Self::new(NaiveDateTime::new(
                date.into_inner().unwrap(),
                second.into_inner().unwrap(),
            )))
        }
    }

    /// Makes a new [`DateTime`] from elapsed seconds since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i32) -> Option<Self> {
        chrono::DateTime::from_timestamp(elapsed as i64, 0)
            .map(|dt| dt.naive_local())
            .map(Self::new)
    }

    /// Counts of seconds since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i32> {
        self.as_ref()
            .map(|t| (t - NaiveDateTime::default()).num_seconds() as i32)
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap()
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%Y.%m.%dT%H:%M:%S")),
        }
    }
}

impl Timestamp {
    /// Makes a new [`Timestamp`] from [`Date`] and [`Time`].
    pub fn from_date_time(date: Date, time: Time) -> Option<Self> {
        if date.is_null() || time.is_null() {
            None
        } else {
            Some(Self::new(NaiveDateTime::new(
                date.into_inner().unwrap(),
                time.into_inner().unwrap(),
            )))
        }
    }

    /// Makes a new [`Timestamp`] from elapsed milliseconds since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Option<Self> {
        chrono::DateTime::from_timestamp_millis(elapsed)
            .map(|dt| dt.naive_local())
            .map(Self::new)
    }

    /// Counts of milliseconds since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .map(|t| (t - NaiveDateTime::default()).num_milliseconds())
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i64 {
        self.elapsed().unwrap()
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%Y.%m.%dT%H:%M:%S%.3f")),
        }
    }
}

impl NanoTime {
    /// Makes a new [`NanoTime`] from hour, minute, second and nanosecond.
    pub fn from_hms_nano(hour: u32, min: u32, sec: u32, nano: u32) -> Option<Self> {
        NaiveTime::from_hms_nano_opt(hour, min, sec, nano).map(Self::new)
    }

    /// Makes a new [`NanoTime`] from elapsed nanoseconds since 00:00:00.
    pub fn from_raw(elapsed: u64) -> Option<Self> {
        let carry = 1_000_000_000;
        NaiveTime::from_num_seconds_from_midnight_opt(
            (elapsed / carry) as u32,
            (elapsed % carry) as u32,
        )
        .map(Self::new)
    }

    /// Counts of nanoseconds since 00:00:00.
    pub fn elapsed(&self) -> Option<u64> {
        self.as_ref().and_then(|t| {
            (t - NaiveTime::default())
                .num_nanoseconds()
                .map(|nsecs| nsecs as u64)
        })
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i64 {
        self.elapsed().unwrap() as i64
    }
}

impl Display for NanoTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%H:%M:%S%.9f")),
        }
    }
}

impl NanoTimestamp {
    /// Makes a new [`NanoTimestamp`] from [`Date`] and [`NanoTime`]
    pub fn from_date_nanotime(date: Date, nanotime: NanoTime) -> Option<Self> {
        if date.is_null() || nanotime.is_null() {
            None
        } else {
            Some(Self::new(NaiveDateTime::new(
                date.into_inner().unwrap(),
                nanotime.into_inner().unwrap(),
            )))
        }
    }

    /// Makes a new [`NanoTime`] from elapsed nanoseconds since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Option<Self> {
        Some(Self::new(
            chrono::DateTime::from_timestamp_nanos(elapsed).naive_local(),
        ))
    }

    /// Counts of nanoseconds since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .and_then(|t| (t - NaiveDateTime::default()).num_nanoseconds())
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i64 {
        self.elapsed().unwrap()
    }
}

impl Display for NanoTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%Y.%m.%dT%H:%M:%S%.9f")),
        }
    }
}

impl DateHour {
    /// Makes a new [`DateHour`] from year, month, day and hour.
    pub fn from_ymd_h(year: i32, month: u32, day: u32, hour: u32) -> Option<Self> {
        let date = NaiveDate::from_ymd_opt(year, month, day);
        let time = NaiveTime::from_hms_opt(hour, 0, 0);

        match (date, time) {
            (Some(d), Some(t)) => Some(Self::new(NaiveDateTime::new(d, t))),
            _ => None,
        }
    }

    /// Makes a new [`DateHour`] from elapsed hours since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Option<Self> {
        chrono::DateTime::from_timestamp(elapsed * 60 * 60, 0)
            .map(|dt| dt.naive_local())
            .map(Self::new)
    }

    /// Counts of hours since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .map(|t| (t - NaiveDateTime::default()).num_hours())
    }

    /// DolphinDB internal representation
    pub(crate) fn ddb_rep(&self) -> i32 {
        self.elapsed().unwrap() as i32
    }
}

impl Display for DateHour {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "null"),
            Some(s) => write!(f, "{}", s.format("%Y.%m.%dT%H")),
        }
    }
}
