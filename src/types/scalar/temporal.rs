use chrono::{naive::NaiveDate, Datelike, Duration};

use super::{
    Date, DateHour, DateTime, Minute, Month, NanoTime, NanoTimeStamp, Second, Time, TimeStamp,
};
use crate::error::RuntimeError;

impl Date {
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
    pub fn from_hms_msec(hour: u32, min: u32, sec: u32, milli: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 || milli > 999 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new(
                (hour * 3600000 + min * 60000 + sec * 1000 + milli) as i32,
            ))
        }
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 / 3600000) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 3600000 / 60000) as u32
    }

    pub fn get_second(&self) -> u32 {
        (self.0 % 60000 / 1000) as u32
    }

    pub fn get_millisecond(&self) -> u32 {
        (self.0 % 1000) as u32
    }
}

impl Minute {
    pub fn from_hm(hour: u32, min: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new((hour * 60 + min) as i32))
        }
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 / 60) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 60) as u32
    }
}

impl Second {
    pub fn from_hms(hour: u32, min: u32, sec: u32) -> Result<Self, RuntimeError> {
        if hour > 23 || min > 59 || sec > 59 {
            Err(RuntimeError::InvalidData)
        } else {
            Ok(Self::new((hour * 3600 + min * 60 + sec) as i32))
        }
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 / 3600) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 3600 / 60) as u32
    }

    pub fn get_second(&self) -> u32 {
        (self.0 % 60) as u32
    }
}

impl DateTime {
    pub fn from_ymd_hms(
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
            date.signed_duration_since(base_date).num_days() as i32 * 86400
                + (hour * 3600 + min * 60 + sec) as i32,
        ))
    }

    pub fn get_year(&self) -> i32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 86400);
        date.year()
    }

    pub fn get_month(&self) -> u32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 86400);
        date.month()
    }

    pub fn get_day(&self) -> u32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 86400);
        date.day()
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 % 86400 / 3600) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 3600 / 60) as u32
    }

    pub fn get_second(&self) -> u32 {
        (self.0 % 60) as u32
    }
}

impl DateHour {
    pub fn from_ymd_hour(year: i32, month: u32, day: u32, hour: u32) -> Result<Self, RuntimeError> {
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

    pub fn get_year(&self) -> i32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 24);
        date.year()
    }

    pub fn get_month(&self) -> u32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 24);
        date.month()
    }

    pub fn get_day(&self) -> u32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 as i64 / 24);
        date.day()
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 % 24) as u32
    }
}

impl TimeStamp {
    pub fn from_ymd_hms_msec(
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

    pub fn get_year(&self) -> i32 {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000);
        date.year()
    }

    pub fn get_month(&self) -> u32 {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000);
        date.month()
    }

    pub fn get_day(&self) -> u32 {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000);
        date.day()
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 % 86400000 / 3600000) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 3600000 / 60000) as u32
    }

    pub fn get_second(&self) -> u32 {
        (self.0 % 60000 / 1000) as u32
    }

    pub fn get_millisecond(&self) -> u32 {
        (self.0 % 1000) as u32
    }
}

impl NanoTime {
    pub fn from_hms_nsec(hour: u32, min: u32, sec: u32, nano: u32) -> Result<Self, RuntimeError> {
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

    pub fn get_hour(&self) -> u32 {
        (self.0 / 3600000000000) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 3600000000000 / 60000000000) as u32
    }

    pub fn get_second(&self) -> u32 {
        (self.0 % 60000000000 / 1000000000) as u32
    }

    pub fn get_nanosecond(&self) -> u32 {
        (self.0 % 1000000000) as u32
    }
}

impl NanoTimeStamp {
    pub fn from_ymd_hms_nsec(
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

    pub fn get_year(&self) -> i32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000000000);
        date.year()
    }

    pub fn get_month(&self) -> u32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000000000);
        date.month()
    }

    pub fn get_day(&self) -> u32 {
        let date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(self.0 / 86400000000000);
        date.day()
    }

    pub fn get_hour(&self) -> u32 {
        (self.0 % 86400000000000 / 3600000000000) as u32
    }

    pub fn get_minute(&self) -> u32 {
        (self.0 % 3600000000000 / 60000000000) as u32
    }

    pub fn get_second(&self) -> u32 {
        (self.0 % 60000000000 / 1000000000) as u32
    }

    pub fn get_nanosecond(&self) -> u32 {
        (self.0 % 1000000000) as u32
    }
}
