use crate::{Date, DateTime};
use chrono::SecondsFormat;
use url::Url;

pub(crate) trait HaystackUrl {
    fn about_url(&self) -> Url;
    fn filetypes_url(&self) -> Url;
    fn his_read_url(&self) -> Url;
    fn his_write_url(&self) -> Url;
    fn nav_url(&self) -> Url;
    fn ops_url(&self) -> Url;
    fn read_url(&self) -> Url;
}

/// Represents the different time range queries that can be sent
/// as part of the `hisRead` Haystack operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HisReadRange {
    /// Query for history values from today.
    Today,
    /// Query for history values from yesterday.
    Yesterday,
    /// Query for history values on a particular date.
    Date(Date),
    /// Query for history values between two dates.
    DateSpan { start: Date, end: Date },
    /// Query for history values between two datetimes.
    DateTimeSpan { start: DateTime, end: DateTime },
    /// Query for history values since a particular datetime.
    SinceDateTime { date_time: DateTime },
}

const DATE_FMT: &str = "%Y-%m-%d";

impl HisReadRange {
    pub(crate) fn to_json_request_string(&self) -> String {
        match self {
            Self::Today => "today".to_owned(),
            Self::Yesterday => "yesterday".to_owned(),
            Self::Date(date) => date.naive_date().format(DATE_FMT).to_string(),
            Self::DateSpan { start, end } => {
                format!(
                    "{},{}",
                    start.naive_date().format(DATE_FMT),
                    end.naive_date().format(DATE_FMT)
                )
            }
            Self::DateTimeSpan { start, end } => {
                let start_str = to_zinc_encoded_string(start);
                let end_str = to_zinc_encoded_string(end);
                format!("{},{}", start_str, end_str)
            }
            Self::SinceDateTime { date_time } => {
                to_zinc_encoded_string(date_time)
            }
        }
    }
}

/// Convert a `DateTime` into a string which can be used in ZINC files.
fn to_zinc_encoded_string(date_time: &DateTime) -> String {
    let time_zone_name = date_time.short_time_zone();
    format!(
        "{} {}",
        date_time
            .date_time()
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        time_zone_name,
    )
}
