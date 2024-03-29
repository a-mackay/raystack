//! # Haystack Types
//! This module defines Haystack types which are not taken from the
//! raystack_core dependency.

use chrono::{NaiveDate, NaiveTime};
use chrono_tz::Tz;
use raystack_core::{FromHaysonError, Hayson};
use serde_json::{json, Value};
use std::convert::From;

const KIND: &str = "_kind";

/// A Haystack Date with no time zone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Date(NaiveDate);

impl Date {
    pub fn new(naive_date: NaiveDate) -> Self {
        Date(naive_date)
    }

    pub fn naive_date(&self) -> &NaiveDate {
        &self.0
    }

    pub fn into_naive_date(self) -> NaiveDate {
        self.0
    }
}

impl Hayson for Date {
    fn from_hayson(value: &Value) -> Result<Self, FromHaysonError> {
        match &value {
            Value::Object(obj) => {
                if let Some(kind_err) = hayson_check_kind("date", value) {
                    return Err(kind_err);
                }
                let val = obj.get("val");

                if val.is_none() {
                    return hayson_error("Date val is missing");
                }

                let val = val.unwrap().as_str();

                if val.is_none() {
                    return hayson_error("Date val is not a string");
                }

                let val = val.unwrap();

                match val.parse() {
                    Ok(naive_date) => Ok(Date::new(naive_date)),
                    Err(_) => hayson_error(
                        "Date val string could not be parsed as a NaiveDate",
                    ),
                }
            }
            _ => hayson_error("Date JSON value must be an object"),
        }
    }

    fn to_hayson(&self) -> Value {
        json!({
            KIND: "date",
            "val": self.naive_date().to_string(),
        })
    }
}

impl From<NaiveDate> for Date {
    fn from(d: NaiveDate) -> Self {
        Self::new(d)
    }
}

/// A Haystack Time with no time zone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Time(NaiveTime);

impl Time {
    pub fn new(naive_time: NaiveTime) -> Self {
        Time(naive_time)
    }

    pub fn naive_time(&self) -> &NaiveTime {
        &self.0
    }

    pub fn into_naive_time(self) -> NaiveTime {
        self.0
    }
}

impl Hayson for Time {
    fn from_hayson(value: &Value) -> Result<Self, FromHaysonError> {
        match &value {
            Value::Object(obj) => {
                if let Some(kind_err) = hayson_check_kind("time", value) {
                    return Err(kind_err);
                }
                let val = obj.get("val");

                if val.is_none() {
                    return hayson_error("Time val is missing");
                }

                let val = val.unwrap().as_str();

                if val.is_none() {
                    return hayson_error("Time val is not a string");
                }

                let val = val.unwrap();

                match val.parse() {
                    Ok(naive_time) => Ok(Time::new(naive_time)),
                    Err(_) => hayson_error(
                        "Time val string could not be parsed as a NaiveTime",
                    ),
                }
            }
            _ => hayson_error("Time JSON value must be an object"),
        }
    }

    fn to_hayson(&self) -> Value {
        json!({
            KIND: "time",
            "val": self.naive_time().to_string(),
        })
    }
}

impl From<NaiveTime> for Time {
    fn from(t: NaiveTime) -> Self {
        Self::new(t)
    }
}

/// A Haystack DateTime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DateTime {
    date_time: chrono::DateTime<Tz>,
}

impl DateTime {
    pub fn new(date_time: chrono::DateTime<Tz>) -> Self {
        Self { date_time }
    }

    pub fn date_time(&self) -> &chrono::DateTime<Tz> {
        &self.date_time
    }

    pub fn into_date_time(self) -> chrono::DateTime<Tz> {
        self.date_time
    }
    /// Return the IANA TZDB identifier, for example  "America/New_York".
    pub fn time_zone(&self) -> &str {
        self.date_time.timezone().name()
    }

    /// Return the short name of the time zone. These
    /// names should match with the shortened time zone names used in
    /// SkySpark.
    pub fn short_time_zone(&self) -> &str {
        crate::tz::time_zone_name_to_short_name(self.time_zone())
    }
}

impl Hayson for DateTime {
    fn from_hayson(value: &Value) -> Result<Self, FromHaysonError> {
        // Use this time zone when the time zone is missing in the Hayson
        // encoding:
        let default_tz = "GMT";

        match &value {
            Value::Object(obj) => {
                if let Some(kind_err) = hayson_check_kind("dateTime", value) {
                    return Err(kind_err);
                }

                let tz_value = obj.get("tz");
                let mut tz_str = default_tz.to_owned();

                if let Some(value) = tz_value {
                    match value {
                        Value::Null => {
                            tz_str = default_tz.to_owned();
                        }
                        Value::String(tz_string) => {
                            tz_str = tz_string.clone();
                        }
                        _ => {
                            return hayson_error(
                                "DateTime tz is not a null or a string",
                            )
                        }
                    }
                }

                let dt = obj.get("val");

                if dt.is_none() {
                    return hayson_error("DateTime val is missing");
                }

                let dt = dt.unwrap().as_str();

                if dt.is_none() {
                    return hayson_error("DateTime val is not a string");
                }

                let dt = dt.unwrap();

                match chrono::DateTime::parse_from_rfc3339(dt) {
                    Ok(dt) => {
                        let tz = crate::skyspark_tz_string_to_tz(&tz_str);
                        if let Some(tz) = tz {
                            let dt = dt.with_timezone(&tz);
                            Ok(DateTime::new(dt))
                        } else {
                            hayson_error(format!("DateTime tz '{}' has no matching chrono_tz time zone", tz_str))
                        }
                    }
                    Err(_) => hayson_error(
                        "Time val string could not be parsed as a NaiveTime",
                    ),
                }
            }
            _ => hayson_error("Time JSON value must be an object"),
        }
    }

    fn to_hayson(&self) -> Value {
        json!({
            KIND: "dateTime",
            "val": self.date_time().to_rfc3339(),
            "tz": self.short_time_zone(),
        })
    }
}

impl From<chrono::DateTime<Tz>> for DateTime {
    fn from(dt: chrono::DateTime<Tz>) -> Self {
        Self::new(dt)
    }
}

fn hayson_error<T, M>(message: M) -> Result<T, FromHaysonError>
where
    M: AsRef<str>,
{
    Err(FromHaysonError::new(message.as_ref().to_owned()))
}

fn hayson_error_opt<M>(message: M) -> Option<FromHaysonError>
where
    M: AsRef<str>,
{
    Some(FromHaysonError::new(message.as_ref().to_owned()))
}

fn hayson_check_kind(
    target_kind: &str,
    value: &Value,
) -> Option<FromHaysonError> {
    match value.get(KIND) {
        Some(kind) => match kind {
            Value::String(kind) => {
                if kind == target_kind {
                    None
                } else {
                    hayson_error_opt(format!(
                        "Expected '{}' = {} but found {}",
                        KIND, kind, kind
                    ))
                }
            }
            _ => hayson_error_opt(format!("'{}' key is not a string", KIND)),
        },
        None => hayson_error_opt(format!("Missing '{}' key", KIND)),
    }
}

#[cfg(test)]
mod test {
    use crate::{Date, DateTime, Time};
    use chrono::{NaiveDate, NaiveTime};
    use chrono_tz::Tz;
    use raystack_core::Hayson;

    #[test]
    fn serde_date_works() {
        let naive_date = NaiveDate::from_ymd(2021, 1, 1);
        let x = Date::new(naive_date);
        let value = x.to_hayson();
        let deserialized = Date::from_hayson(&value).unwrap();
        assert_eq!(x, deserialized);
    }

    #[test]
    fn serde_time_works() {
        let naive_time = NaiveTime::from_hms(2, 15, 59);
        let x = Time::new(naive_time);
        let value = x.to_hayson();
        let deserialized = Time::from_hayson(&value).unwrap();
        assert_eq!(x, deserialized);
    }

    #[test]
    fn serde_date_time_works() {
        let dt =
            chrono::DateTime::parse_from_rfc3339("2021-01-01T18:30:09.453Z")
                .unwrap()
                .with_timezone(&Tz::GMT);
        let x = DateTime::new(dt);
        let value = x.to_hayson();
        let deserialized = DateTime::from_hayson(&value).unwrap();
        assert_eq!(x, deserialized);
    }

    #[test]
    fn serde_date_time_with_one_slash_tz_works() {
        let dt =
            chrono::DateTime::parse_from_rfc3339("2021-01-01T18:30:09.453Z")
                .unwrap()
                .with_timezone(&Tz::Australia__Sydney);
        let x = DateTime::new(dt);
        let value = x.to_hayson();
        let deserialized = DateTime::from_hayson(&value).unwrap();
        assert_eq!(x, deserialized);
    }

    #[test]
    fn serde_date_time_with_multiple_slashes_tz_works() {
        let dt =
            chrono::DateTime::parse_from_rfc3339("2021-01-01T18:30:09.453Z")
                .unwrap()
                .with_timezone(&Tz::America__North_Dakota__Beulah);
        let x = DateTime::new(dt);
        let value = x.to_hayson();
        let deserialized = DateTime::from_hayson(&value).unwrap();
        assert_eq!(x, deserialized);
    }

    #[test]
    fn short_time_zone_works() {
        let dt: DateTime =
            chrono::DateTime::parse_from_rfc3339("2021-01-01T18:30:09.453Z")
                .unwrap()
                .with_timezone(&Tz::America__North_Dakota__Beulah)
                .into();
        assert_eq!(dt.short_time_zone(), "Beulah");

        let dt: DateTime =
            chrono::DateTime::parse_from_rfc3339("2021-01-01T18:30:09.453Z")
                .unwrap()
                .with_timezone(&Tz::GMT)
                .into();
        assert_eq!(dt.short_time_zone(), "GMT");

        let dt: DateTime =
            chrono::DateTime::parse_from_rfc3339("2021-01-01T18:30:09.453Z")
                .unwrap()
                .with_timezone(&Tz::Australia__Sydney)
                .into();
        assert_eq!(dt.short_time_zone(), "Sydney");
    }
}
