//! # Haystack Types
//! This module defines Haystack types which are not taken from the
//! raystack_core dependency.

use chrono::{NaiveDate, NaiveTime};
use raystack_core::{FromHaysonError, Hayson};
use serde_json::{json, Value};

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
                if let Some(kind_err) = hayson_check_kind("date", &value) {
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
                    Err(_) => hayson_error("Date val string could not be parsed as a NaiveDate")
                }
            },
            _ => hayson_error("Date JSON value must be an object")
        }
    }

    fn to_hayson(&self) -> Value {
        json!({
            KIND: "date",
            "val": self.naive_date().to_string(),
        })
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
                if let Some(kind_err) = hayson_check_kind("time", &value) {
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
                    Err(_) => hayson_error("Time val string could not be parsed as a NaiveTime")
                }
            },
            _ => hayson_error("Time JSON value must be an object")
        }
    }

    fn to_hayson(&self) -> Value {
        json!({
            KIND: "time",
            "val": self.naive_time().to_string(),
        })
    }
}

fn hayson_error<T, M>(message: M) -> Result<T, FromHaysonError> where M: AsRef<str> {
    Err(FromHaysonError::new(message.as_ref().to_owned()))
}

fn hayson_error_opt<M>(message: M) -> Option<FromHaysonError> where M: AsRef<str> {
    Some(FromHaysonError::new(message.as_ref().to_owned()))
}

fn hayson_check_kind(target_kind: &str, value: &Value) -> Option<FromHaysonError> {
    match value.get(KIND) {
        Some(kind) => {
            match kind {
                Value::String(kind) => {
                    if kind == target_kind {
                        None
                    } else {
                        hayson_error_opt(format!("Expected '{}' = {} but found {}", KIND, kind, kind))
                    }
                },
                _ => hayson_error_opt(format!("'{}' key is not a string", KIND))
            }
        },
        None => hayson_error_opt(format!("Missing '{}' key", KIND)),
    }
}


#[cfg(test)]
mod test {
    use raystack_core::Hayson;
    use chrono::{NaiveDate, NaiveTime};
    use crate::{Date, Time};

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
}