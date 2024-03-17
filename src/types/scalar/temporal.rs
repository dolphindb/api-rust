use chrono::{
    naive::{NaiveDate, NaiveDateTime, NaiveTime},
    Datelike, Duration,
};

use super::{
    Date, DateHour, DateTime, Minute, Month, NanoTime, NanoTimeStamp, Second, Time, TimeStamp,
};
use crate::error::RuntimeError;

impl Date {
    /// Makes a new [`Date`] from the calendar date (year, month and day).
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self, RuntimeError> {
        let date = NaiveDate::from_ymd_opt(year, month, day).ok_or(RuntimeError::InvalidData)?;
        let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Ok(Self::new(
            date.signed_duration_since(base_date).num_days() as i32
        ))
    }

    pub fn get_year(&self) -> i32 {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64);
        date.year()
    }

    pub fn get_month(&self) -> u32 {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64);
        date.month()
    }

    pub fn get_day(&self) -> u32 {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64);
        date.day()
    }
}

impl Month {
    /// Makes a new [`Month`] from the calendar month (year, month).
    pub fn from_ym(year: i32, month: u32) -> Result<Self, RuntimeError> {
        if month > 12 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new(year * 12 + month as i32))
        }
    }

    pub fn get_year(&self) -> i32 {
        self.0 / 12
    }

    pub fn get_month(&self) -> i32 {
        self.0 % 12 + 1
    }
}

impl Time {
    /// Makes a new [`Time`] from hour, minute, second and millisecond.
    pub fn from_time(hour: u32, min: u32, sec: u32, milli: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 || milli > 999 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new(
                (hour * 3600000 + min * 60000 + sec * 1000 + milli) as i32,
            ))
        }
    }
}

impl Minute {
    /// Makes a new [`Minute`] from hour, minute.
    pub fn from_hm(hour: u32, min: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new((hour * 60 + min) as i32))
        }
    }
}

impl Second {
    /// Makes a new [`Minute`] from hour, minute and second.
    pub fn from_hms(hour: u32, min: u32, sec: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new((hour * 3600 + min * 60 + sec) as i32))
        }
    }
}

impl DateTime {
    pub fn from_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
    ) -> Result<Self, RuntimeError> {
        let date = NaiveDate::from_ymd_opt(year, month, day).ok_or(RuntimeError::InvalidData)?;
        let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Ok(Self::new(
            date.signed_duration_since(base_date).num_days() as i32
                + (hour * 3600 + min * 60 + sec) as i32,
        ))
    }
}

impl TimeStamp {
    pub fn from_timestamp(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        milli: u32,
    ) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 || milli > 999 {
            Err(RuntimeError::InvalidData)
        } else {
            let date =
                NaiveDate::from_ymd_opt(year, month, day).ok_or(RuntimeError::InvalidData)?;
            let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Ok(Self::new(
                date.signed_duration_since(base_date).num_days() * 86400000
                    + (hour * 3600000 + min * 60000 + sec * 1000 + milli) as i64,
            ))
        }
    }
}

impl NanoTime {
    /// Makes a new [`NanoTime`] from hour, minute, second and nanosecond.
    pub fn from_hms_nano(hour: u32, min: u32, sec: u32, nano: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 || nano > 999999999 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new(
                hour as i64 * 3600000000000
                    + min as i64 * 60000000000
                    + sec as i64 * 1000000000
                    + nano as i64,
            ))
        }
    }
}

impl NanoTimeStamp {
    pub fn from_nano_timestamp(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        nano: u32,
    ) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 || nano > 999999999 {
            Err(RuntimeError::InvalidData)
        } else {
            let date =
                NaiveDate::from_ymd_opt(year, month, day).ok_or(RuntimeError::InvalidData)?;
            let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Ok(Self::new(
                date.signed_duration_since(base_date).num_days() * 86400000000000
                    + hour as i64 * 3600000000000
                    + min as i64 * 60000000000
                    + sec as i64 * 1000000000
                    + nano as i64,
            ))
        }
    }
}

impl DateHour {
    /// Makes a new [`DateHour`] from year, month, day and hour.
    pub fn from_datehour(year: i32, month: u32, day: u32, hour: u32) -> Result<Self, RuntimeError> {
        if hour > 23 {
            Err(RuntimeError::InvalidData)
        } else {
            let date =
                NaiveDate::from_ymd_opt(year, month, day).ok_or(RuntimeError::InvalidData)?;
            let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Ok(Self::new(
                (date.signed_duration_since(base_date).num_days() * 24) as i32 + hour as i32,
            ))
        }
    }
}
