//! Temporal type interface compatible to [`chrono`]

use chrono::{
    naive::{NaiveDate, NaiveDateTime, NaiveTime},
    Datelike, Days, Months,
};

use super::super::*;

impl Date {
    /// Makes a new [`Date`] from the calendar date (year, month and day).
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Self {
        Self::new(NaiveDate::from_ymd_opt(year, month, day))
    }

    /// Makes a new [`Date`] from elapsed day since 1st of January 1970.
    pub fn from_raw(elapsed: i64) -> Self {
        if elapsed >= 0 {
            Self::new(NaiveDate::default().checked_add_days(Days::new(elapsed as u64)))
        } else {
            Self::new(NaiveDate::default().checked_sub_days(Days::new(elapsed.unsigned_abs())))
        }
    }

    /// Counts of day since 1st of January 1970.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .map(|d| (*d - NaiveDate::default()).num_days())
    }
}

impl Month {
    /// Makes a new [`Month`] from the calendar month (year, month).
    pub fn from_ym(year: i32, month: u32) -> Self {
        Self::new(NaiveDate::from_ymd_opt(year, month, 1))
    }

    /// Makes a new [`Month`] from elapsed month since January 1970.
    pub fn from_raw(elapsed: i32) -> Self {
        if elapsed >= 0 {
            Self::new(NaiveDate::default().checked_add_months(Months::new(elapsed as u32)))
        } else {
            Self::new(NaiveDate::default().checked_sub_months(Months::new(elapsed.unsigned_abs())))
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
}

impl Time {
    /// Makes a new [`Time`] from hour, minute, second and millisecond.
    pub fn from_hms_milli(hour: u32, min: u32, sec: u32, milli: u32) -> Self {
        Self::new(NaiveTime::from_hms_milli_opt(hour, min, sec, milli))
    }

    /// Makes a new [`Time`] from elapsed milliseconds since 00:00:00.000.
    pub fn from_raw(elapsed: u32) -> Self {
        Self::new(NaiveTime::from_num_seconds_from_midnight_opt(
            elapsed / 1_000,
            elapsed % 1_000 * 1_000_000,
        ))
    }

    /// Counts of milliseconds since 00:00:00.000.
    pub fn elapsed(&self) -> Option<u32> {
        self.as_ref()
            .map(|t| (*t - NaiveTime::default()).num_milliseconds() as u32)
    }
}

impl Minute {
    /// Makes a new [`Minute`] from hour, minute.
    pub fn from_hm(hour: u32, min: u32) -> Self {
        Self::new(NaiveTime::from_hms_opt(hour, min, 0))
    }

    /// Makes a new [`Minute`] from elapsed minutes since 00:00.
    pub fn from_raw(elapsed: u32) -> Self {
        Self::new(NaiveTime::from_num_seconds_from_midnight_opt(
            elapsed * 60,
            0,
        ))
    }

    /// Counts of minutes since 00:00.
    pub fn elapsed(&self) -> Option<u32> {
        self.as_ref()
            .map(|t| (*t - NaiveTime::default()).num_minutes() as u32)
    }
}

impl Second {
    /// Makes a new [`Minute`] from hour, minute and second.
    pub fn from_hms(hour: u32, min: u32, sec: u32) -> Self {
        Self::new(NaiveTime::from_hms_opt(hour, min, sec))
    }

    /// Makes a new [`Second`] from elapsed seconds since 00:00:00.
    pub fn from_raw(elapsed: u32) -> Self {
        Self::new(NaiveTime::from_num_seconds_from_midnight_opt(elapsed, 0))
    }

    /// Counts of seconds since 00:00:00.
    pub fn elapsed(&self) -> Option<u32> {
        self.as_ref()
            .map(|t| (*t - NaiveTime::default()).num_seconds() as u32)
    }
}

impl DateTime {
    /// Makes a new [`DateTime`] from [`Date`] and [`Second`].
    pub fn from_date_second(date: Date, second: Second) -> Self {
        if date.is_null() || second.is_null() {
            Self(None)
        } else {
            Self::new(Some(NaiveDateTime::new(
                date.into_inner().unwrap(),
                second.into_inner().unwrap(),
            )))
        }
    }

    /// Makes a new [`DateTime`] from elapsed seconds since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Self {
        Self::new(NaiveDateTime::from_timestamp_opt(elapsed, 0))
    }

    /// Counts of seconds since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i32> {
        self.as_ref()
            .map(|t| (*t - NaiveDateTime::default()).num_seconds() as i32)
    }
}

impl TimeStamp {
    /// Makes a new [`TimeStamp`] from [`Date`] and [`Time`].
    pub fn from_date_time(date: Date, time: Time) -> Self {
        if date.is_null() || time.is_null() {
            Self(None)
        } else {
            Self::new(Some(NaiveDateTime::new(
                date.into_inner().unwrap(),
                time.into_inner().unwrap(),
            )))
        }
    }

    /// Makes a new [`TimeStamp`] from elapsed milliseconds since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Self {
        Self::new(NaiveDateTime::from_timestamp_millis(elapsed))
    }

    /// Counts of milliseconds since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .map(|t| (*t - NaiveDateTime::default()).num_milliseconds())
    }
}

impl NanoTime {
    /// Makes a new [`NanoTime`] from hour, minute, second and nanosecond.
    pub fn from_hms_nano(hour: u32, min: u32, sec: u32, nano: u32) -> Self {
        Self::new(NaiveTime::from_hms_nano_opt(hour, min, sec, nano))
    }

    /// Makes a new [`NanoTime`] from elapsed nanoseconds since 00:00:00.
    pub fn from_raw(elapsed: u64) -> Self {
        let carry = 1_000_000_000;
        Self::new(NaiveTime::from_num_seconds_from_midnight_opt(
            (elapsed / carry) as u32,
            (elapsed % carry) as u32,
        ))
    }

    /// Counts of nanoseconds since 00:00:00.
    pub fn elapsed(&self) -> Option<u64> {
        self.as_ref().and_then(|t| {
            (*t - NaiveTime::default())
                .num_nanoseconds()
                .map(|nsecs| nsecs as u64)
        })
    }
}

impl NanoTimeStamp {
    /// Makes a new [`NanoTimeStamp`] from [`Date`] and [`NanoTime`]
    pub fn from_date_nanotime(date: Date, nanotime: NanoTime) -> Self {
        if date.is_null() || nanotime.is_null() {
            Self(None)
        } else {
            Self::new(Some(NaiveDateTime::new(
                date.into_inner().unwrap(),
                nanotime.into_inner().unwrap(),
            )))
        }
    }

    /// Makes a new [`NanoTime`] from elapsed nanoseconds since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Self {
        Self::new(NaiveDateTime::from_timestamp_nanos(elapsed))
    }

    /// Counts of nanoseconds since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .and_then(|t| (*t - NaiveDateTime::default()).num_nanoseconds())
    }
}

impl DateHour {
    /// Makes a new [`DateHour`] from year, month, day and hour.
    pub fn from_ymd_h(year: i32, month: u32, day: u32, hour: u32) -> Self {
        let date = NaiveDate::from_ymd_opt(year, month, day);
        let time = NaiveTime::from_hms_opt(hour, 0, 0);

        match (date, time) {
            (Some(d), Some(t)) => Self::new(Some(NaiveDateTime::new(d, t))),
            _ => Self(None),
        }
    }

    /// Makes a new [`DateHour`] from elapsed hours since 1st of January 1970 at 00:00:00.
    pub fn from_raw(elapsed: i64) -> Self {
        Self::new(NaiveDateTime::from_timestamp_opt(elapsed * 60 * 60, 0))
    }

    /// Counts of hours since 1st of January 1970 at 00:00:00.
    pub fn elapsed(&self) -> Option<i64> {
        self.as_ref()
            .map(|t| (*t - NaiveDateTime::default()).num_hours())
    }
}
