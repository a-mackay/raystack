use crate::grid::Grid;
use crate::hsref::Ref;
use crate::Error;
use chrono::{DateTime, NaiveDate, SecondsFormat};
use chrono_tz::Tz;
use url::Url;

/// Provides functions which correspond to some Haystack REST API operations.
trait HaystackRest {
    /// Returns a grid containing basic server information.
    fn about(&self) -> Result<Grid, Error>;
    /// Returns a grid describing what MIME types are available.
    fn formats(&self) -> Result<Grid, Error>;
    /// Returns a grid of history data for a single point.
    fn his_read(&self, id: &Ref, range: &HisReadRange) -> Result<Grid, Error>;
    /// Writes boolean values to a single point.
    fn his_write_bool(
        &self,
        id: &Ref,
        his_data: &[(DateTime<Tz>, bool)],
    ) -> Result<Grid, Error>;
    /// Writes string values to a single point.
    fn his_write_str(
        &self,
        id: &Ref,
        his_data: &[(DateTime<Tz>, String)],
    ) -> Result<Grid, Error>;
    /// Writes numeric values to a single point. `unit` must be a valid
    /// Haystack unit literal, such as `L/s` or `celsius`.
    fn his_write_num(
        &self,
        id: &Ref,
        his_data: &[(DateTime<Tz>, f64)],
        unit: &str,
    ) -> Result<Grid, Error>;
    /// The Haystack nav operation.
    fn nav(&self, nav_id: Option<&str>) -> Result<Grid, Error>;
    /// Returns a grid containing the operations available on the server.
    fn ops(&self) -> Result<Grid, Error>;
    /// Returns a grid containing the records matching the given Axon
    /// filter string.
    fn read(&self, filter: &str, limit: Option<u64>) -> Result<Grid, Error>;
    /// Returns a grid containing the records matching the given id
    /// `Ref`s.
    fn read_by_ids(&self, ids: &[Ref]) -> Result<Grid, Error>;
}

pub(crate) trait HaystackUrl {
    fn about_url(&self) -> Url;
    fn formats_url(&self) -> Url;
    fn his_read_url(&self) -> Url;
    fn his_write_url(&self) -> Url;
    fn nav_url(&self) -> Url;
    fn ops_url(&self) -> Url;
    fn read_url(&self) -> Url;
}

/// Provides functions which correspond to some SkySpark REST API operations.
trait SkySparkRest {
    /// Evaluate an Axon expression on the SkySpark server and return a grid
    /// containing the resulting data.
    fn eval(&self, axon_expr: &str) -> Result<Grid, Error>;
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
    Date(NaiveDate),
    /// Query for history values between two dates.
    DateSpan { start: NaiveDate, end: NaiveDate },
    /// Query for history values between two datetimes.
    DateTimeSpan {
        start: DateTime<Tz>,
        end: DateTime<Tz>,
    },
    /// Query for history values since a particular datetime.
    SinceDateTime { date_time: DateTime<Tz> },
}

const DATE_FMT: &str = "%Y-%m-%d";

impl HisReadRange {
    pub(crate) fn to_string(&self) -> String {
        match self {
            Self::Today => "today".to_owned(),
            Self::Yesterday => "yesterday".to_owned(),
            Self::Date(date) => date.format(DATE_FMT).to_string(),
            Self::DateSpan { start, end } => {
                format!("{},{}", start.format(DATE_FMT), end.format(DATE_FMT))
            }
            Self::DateTimeSpan { start, end } => {
                let start_str = to_zinc_encoded_string(&start);
                let end_str = to_zinc_encoded_string(&end);
                format!("{},{}", start_str, end_str)
            }
            Self::SinceDateTime { date_time } => {
                to_zinc_encoded_string(&date_time)
            }
        }
    }
}

/// Convert a `DateTime` into a string which can be used in ZINC files.
pub(crate) fn to_zinc_encoded_string(date_time: &DateTime<Tz>) -> String {
    let time_zone_name = *date_time
        .timezone()
        .name()
        .split('/')
        .collect::<Vec<_>>()
        .last()
        .expect("timezone name is always formatted as Area/Location");
    format!(
        "{} {}",
        date_time.to_rfc3339_opts(SecondsFormat::Secs, true),
        time_zone_name,
    )
}
