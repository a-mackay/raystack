use chrono::{DateTime, FixedOffset};
use crate::{Date, Time};
use raystack_core::{Coord, Hayson, Marker, Na, Number, RemoveMarker, Ref, Symbol, Uri, Xstr};
use serde_json::Value;

/// An extension trait for the `serde_json::Value` enum,
/// containing helper functions which make it easier to
/// parse specific Haystack types from the underlying Hayson encoding
/// (a JSON value in a specific format, see https://github.com/j2inn/hayson).
pub trait ValueExt {
    /// Convert the JSON value to a Haystack Coord.
    fn as_hs_coord(&self) -> Option<Coord>;
    /// Convert the JSON value to a Haystack Date.
    fn as_hs_date(&self) -> Option<Date>;
    /// Convert the JSON value to a tuple containing a
    /// DateTime with a fixed timezone offset, and a string
    /// containing the Haystack timezone name.
    fn as_hs_date_time(&self) -> Option<(DateTime<FixedOffset>, &str)>;
    /// Convert the JSON value to a Haystack Marker.
    fn as_hs_marker(&self) -> Option<Marker>;
    /// Convert the JSON value to a Haystack NA.
    fn as_hs_na(&self) -> Option<Na>;
    /// Convert the JSON value to a Haystack Number.
    fn as_hs_number(&self) -> Option<Number>;
    /// Convert the JSON value to a Haystack Ref.
    fn as_hs_ref(&self) -> Option<Ref>;
    /// Convert the JSON value to a Haystack Remove Marker.
    fn as_hs_remove_marker(&self) -> Option<RemoveMarker>;
    /// Parse the JSON value as a Haystack Str.
    fn as_hs_str(&self) -> Option<&str>;
    /// Convert the JSON value to a Haystack Symbol.
    fn as_hs_symbol(&self) -> Option<Symbol>;
    /// Convert the JSON value to a Haystack Time.
    fn as_hs_time(&self) -> Option<Time>;
    /// Returns the Haystack URI value as a Haystack Uri.
    fn as_hs_uri(&self) -> Option<Uri>;
    /// Return the Haystack XStr value as a Haystack Xstr.
    fn as_hs_xstr(&self) -> Option<Xstr>;
    /// Returns true if the JSON value represents a Haystack
    /// Coord.
    fn is_hs_coord(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// Date.
    fn is_hs_date(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// DateTime.
    fn is_hs_date_time(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// marker.
    fn is_hs_marker(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// NA value.
    fn is_hs_na(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// Number.
    fn is_hs_number(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// Ref.
    fn is_hs_ref(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// remove marker.
    fn is_hs_remove_marker(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// Str.
    fn is_hs_str(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// Symbol.
    fn is_hs_symbol(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// Time.
    fn is_hs_time(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// URI.
    fn is_hs_uri(&self) -> bool;
    /// Returns true if the JSON value represents a Haystack
    /// XStr.
    fn is_hs_xstr(&self) -> bool;
}

impl ValueExt for Value {
    fn as_hs_coord(&self) -> Option<Coord> {
        Coord::from_hayson(self).ok()
    }

    fn as_hs_date(&self) -> Option<Date> {
        Date::from_hayson(self).ok()
    }

    fn as_hs_date_time(&self) -> Option<(DateTime<FixedOffset>, &str)> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::DateTime => {
                let mut split = trim_hs_prefix(s).split(' ');
                let date_time = split
                    .next()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
                let time_zone_name = split.next();
                let tuple = (date_time, time_zone_name);
                match tuple {
                    (Some(date_time), Some(time_zone_name)) => {
                        Some((date_time, time_zone_name))
                    }
                    _ => None,
                }
            }
            _ => None,
        })
    }

    fn as_hs_marker(&self) -> Option<Marker> {
        Marker::from_hayson(self).ok()
    }

    fn as_hs_na(&self) -> Option<Na> {
        Na::from_hayson(self).ok()
    }

    fn as_hs_number(&self) -> Option<Number> {
        Number::from_hayson(self).ok()
    }

    fn as_hs_ref(&self) -> Option<Ref> {
        Ref::from_hayson(self).ok()
    }

    fn as_hs_remove_marker(&self) -> Option<RemoveMarker> {
        RemoveMarker::from_hayson(self).ok()
    }

    fn as_hs_str(&self) -> Option<&str> {
        self.as_str()
    }

    fn as_hs_symbol(&self) -> Option<Symbol> {
        Symbol::from_hayson(self).ok()
    }

    fn as_hs_time(&self) -> Option<Time> {
        Time::from_hayson(self).ok()
    }

    fn as_hs_uri(&self) -> Option<Uri> {
        Uri::from_hayson(self).ok()
    }

    fn as_hs_xstr(&self) -> Option<Xstr> {
        Xstr::from_hayson(self).ok()
    }

    fn is_hs_coord(&self) -> bool {
        self.as_hs_coord().is_some()
    }

    fn is_hs_date(&self) -> bool {
        self.as_hs_date().is_some()
    }

    fn is_hs_date_time(&self) -> bool {
        self.as_hs_date_time().is_some()
    }

    fn is_hs_marker(&self) -> bool {
        self.as_hs_marker().is_some()
    }

    fn is_hs_na(&self) -> bool {
        self.as_hs_na().is_some()
    }

    fn is_hs_number(&self) -> bool {
        self.as_hs_number().is_some()
    }

    fn is_hs_ref(&self) -> bool {
        self.as_hs_ref().is_some()
    }

    fn is_hs_remove_marker(&self) -> bool {
        self.as_hs_remove_marker().is_some()
    }

    fn is_hs_str(&self) -> bool {
        self.as_hs_str().is_some()
    }

    fn is_hs_symbol(&self) -> bool {
        self.as_hs_symbol().is_some()
    }

    fn is_hs_time(&self) -> bool {
        self.as_hs_time().is_some()
    }

    fn is_hs_uri(&self) -> bool {
        self.as_hs_uri().is_some()
    }

    fn is_hs_xstr(&self) -> bool {
        self.as_hs_xstr().is_some()
    }
}

#[cfg(test)]
mod test {
    use super::ValueExt;
    use serde_json::json;

    #[test]
    fn as_hs_str() {
        let plain_val = json!("hello world");
        let prefixed_val = json!("s:hello world");

        assert_eq!(plain_val.as_hs_str(), Some("hello world"));
        assert_eq!(prefixed_val.as_hs_str(), Some("hello world"));
    }

    #[test]
    fn as_hs_ref() {
        let ref_val = json!("r:abc-123");
        let ref_val_and_display_name = json!("r:abc-123 RTU #3");
        assert_eq!(ref_val.as_hs_ref().unwrap().as_ref(), "@abc-123");
        assert_eq!(
            ref_val_and_display_name.as_hs_ref().unwrap().as_ref(),
            "@abc-123"
        );
    }

    #[test]
    fn as_hs_number() {
        use crate::Number;

        let number_val = json!("n:25.123 celsius");
        assert_eq!(
            number_val.as_hs_number().unwrap(),
            Number::new(25.123, Some("celsius".to_owned()))
        );

        let number_no_unit_val = json!("n:25.123");
        assert_eq!(
            number_no_unit_val.as_hs_number().unwrap(),
            Number::new(25.123, None)
        );
    }

    #[test]
    fn as_hs_date() {
        use chrono::NaiveDate;

        let number_val = json!("d:2014-12-01");
        assert_eq!(
            number_val.as_hs_date().unwrap(),
            NaiveDate::from_ymd(2014, 12, 1)
        );
    }

    #[test]
    fn as_hs_time() {
        use chrono::NaiveTime;

        let time_val = json!("h:23:59");
        let time = time_val.as_hs_time().unwrap();
        assert_eq!(time, NaiveTime::from_hms(23, 59, 0));
    }

    #[test]
    fn as_hs_time_with_seconds() {
        use chrono::NaiveTime;

        let time_val = json!("h:23:59:15");
        let time = time_val.as_hs_time().unwrap();
        assert_eq!(time, NaiveTime::from_hms(23, 59, 15));
    }

    #[test]
    fn as_hs_time_with_no_hour_padding() {
        use chrono::NaiveTime;

        let time_val = json!("h:3:59");
        let time = time_val.as_hs_time().unwrap();
        assert_eq!(time, NaiveTime::from_hms(3, 59, 0));
    }

    #[test]
    fn as_hs_time_with_no_hour_padding_and_seconds() {
        use chrono::NaiveTime;

        let time_val = json!("h:3:59:15");
        let time = time_val.as_hs_time().unwrap();
        assert_eq!(time, NaiveTime::from_hms(3, 59, 15));
    }

    #[test]
    fn as_hs_uri() {
        let uri_val = json!("u:www.test.com");
        let uri = uri_val.as_hs_uri().unwrap();
        assert_eq!(uri, "www.test.com");
    }

    #[test]
    fn as_hs_xstr() {
        let xstr_val = json!("x:Type:value");
        let xstr = xstr_val.as_hs_xstr().unwrap();
        assert_eq!(xstr, "Type:value");
    }

    #[test]
    fn as_hs_coord() {
        use crate::Coord;

        let coord_val = json!("c:37.545,-77.449");
        let coord = coord_val.as_hs_coord().unwrap();
        assert_eq!(coord, Coord::new(37.545, -77.449));
    }

    #[test]
    fn as_hs_date_time() {
        use chrono::{FixedOffset, TimeZone};
        let hour = 3600;

        let dt_val = json!("t:2015-06-08T15:47:41-04:00 New_York");
        let (dt, tz_name) = dt_val.as_hs_date_time().unwrap();
        assert_eq!(tz_name, "New_York");
        assert_eq!(
            dt,
            FixedOffset::west(4 * hour)
                .ymd(2015, 6, 8)
                .and_hms(15, 47, 41),
        );
    }
}
