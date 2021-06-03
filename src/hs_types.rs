//! # Haystack Types
//! This module defines Haystack types which are not taken from the
//! raystack_core dependency.

use chrono::NaiveDate;
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