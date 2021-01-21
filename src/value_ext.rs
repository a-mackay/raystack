use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use raystack_core::{Coord, Number, Ref};
use serde_json::Value;

/// An extension trait for the `serde_json::Value` enum,
/// containing helper functions which make it easier to
/// parse specific Haystack types from the underlying JSON
/// value.
pub trait ValueExt {
    /// Convert the JSON value to a Haystack Coord.
    fn as_hs_coord(&self) -> Option<Coord>;
    /// Convert the JSON value to a Haystack Date.
    fn as_hs_date(&self) -> Option<NaiveDate>;
    /// Convert the JSON value to a tuple containing a
    /// DateTime with a fixed timezone offset, and a string
    /// containing the Haystack timezone name.
    fn as_hs_date_time(&self) -> Option<(DateTime<FixedOffset>, &str)>;
    /// Convert the JSON value to a Haystack Number.
    fn as_hs_number(&self) -> Option<Number>;
    /// Convert the JSON value to a Haystack Ref.
    fn as_hs_ref(&self) -> Option<Ref>;
    /// Parse the JSON value as a Haystack Str, removing
    /// the "s:" prefix if necessary.
    fn as_hs_str(&self) -> Option<&str>;
    /// Convert the JSON value to a Haystack Time.
    fn as_hs_time(&self) -> Option<NaiveTime>;
    /// Returns the Haystack URI value as a string.
    fn as_hs_uri(&self) -> Option<&str>;
    /// Return the Haystack XStr value as a string.
    fn as_hs_xstr(&self) -> Option<&str>;
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
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::Coord => {
                let mut split = trim_hs_prefix(s).split(',');
                let lat = split.next().and_then(|s| str::parse(s).ok());
                let lng = split.next().and_then(|s| str::parse(s).ok());
                match (lat, lng) {
                    (Some(lat), Some(lng)) => Some(Coord::new(lat, lng)),
                    _ => None,
                }
            }
            _ => None,
        })
    }

    fn as_hs_date(&self) -> Option<NaiveDate> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::Date => {
                let date_str = trim_hs_prefix(s);
                NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
            }
            _ => None,
        })
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

    fn as_hs_number(&self) -> Option<Number> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::Number => {
                Number::from_encoded_json_string(s).ok()
            }
            _ => None,
        })
    }

    fn as_hs_ref(&self) -> Option<Ref> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::Ref => {
                Ref::from_encoded_json_string(s).ok()
            }
            _ => None,
        })
    }

    fn as_hs_str(&self) -> Option<&str> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::PlainString => Some(s),
            JsonStringHaystackType::PrefixedString => Some(trim_hs_prefix(s)),
            _ => None,
        })
    }

    fn as_hs_time(&self) -> Option<NaiveTime> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::Time => {
                let time_str = trim_hs_prefix(s);
                NaiveTime::parse_from_str(time_str, "%k:%M:%S")
                    .ok()
                    .or_else(|| {
                        NaiveTime::parse_from_str(time_str, "%k:%M").ok()
                    })
            }
            _ => None,
        })
    }

    fn as_hs_uri(&self) -> Option<&str> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::Uri => Some(trim_hs_prefix(s)),
            _ => None,
        })
    }

    fn as_hs_xstr(&self) -> Option<&str> {
        self.as_str().and_then(|s| match haystack_type(s) {
            JsonStringHaystackType::XStr => Some(trim_hs_prefix(s)),
            _ => None,
        })
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
        if let Some(s) = self.as_str() {
            matches!(haystack_type(s), JsonStringHaystackType::Marker)
        } else {
            false
        }
    }

    fn is_hs_na(&self) -> bool {
        if let Some(s) = self.as_str() {
            matches!(haystack_type(s), JsonStringHaystackType::Na)
        } else {
            false
        }
    }

    fn is_hs_number(&self) -> bool {
        self.as_hs_number().is_some()
    }

    fn is_hs_ref(&self) -> bool {
        self.as_hs_ref().is_some()
    }

    fn is_hs_remove_marker(&self) -> bool {
        if let Some(s) = self.as_str() {
            matches!(haystack_type(s), JsonStringHaystackType::RemoveMarker)
        } else {
            false
        }
    }

    fn is_hs_str(&self) -> bool {
        self.as_hs_str().is_some()
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

/// Determine the Haystack type of the given string by
/// looking for specific prefix characters.
fn haystack_type(s: &str) -> JsonStringHaystackType {
    if let Some(prefix) = first_two_chars(s) {
        match prefix.as_ref() {
            "m:" => JsonStringHaystackType::Marker,
            "-:" => JsonStringHaystackType::RemoveMarker,
            "z:" => JsonStringHaystackType::Na,
            "n:" => JsonStringHaystackType::Number,
            "r:" => JsonStringHaystackType::Ref,
            "s:" => JsonStringHaystackType::PrefixedString,
            "d:" => JsonStringHaystackType::Date,
            "h:" => JsonStringHaystackType::Time,
            "t:" => JsonStringHaystackType::DateTime,
            "u:" => JsonStringHaystackType::Uri,
            "c:" => JsonStringHaystackType::Coord,
            "x:" => JsonStringHaystackType::XStr,
            _ => JsonStringHaystackType::PlainString,
        }
    } else {
        JsonStringHaystackType::PlainString
    }
}

/// If possible, return the first two characters of the
/// original string. Otherwise, return `None`.
fn first_two_chars(s: &str) -> Option<String> {
    let prefix: String = s.chars().take(2).collect();
    if prefix.chars().count() == 2 {
        Some(prefix)
    } else {
        None
    }
}

/// Return a string slice with the first two characters removed.
fn trim_hs_prefix(s: &str) -> &str {
    // Since we know the first two chars are valid ASCII chars,
    // we can trim the first two bytes and still have a valid
    // UTF8 string:
    &s[2..]
}

/// Haystack types which are represented as JSON strings.
#[derive(Clone, Debug, Eq, PartialEq)]
enum JsonStringHaystackType {
    Marker,
    RemoveMarker,
    Na,
    Number,
    Ref,
    PlainString,
    PrefixedString,
    Date,
    Time,
    DateTime,
    Uri,
    Coord,
    XStr,
}

#[cfg(test)]
mod test {
    use super::ValueExt;
    use serde_json::json;

    #[test]
    fn haystack_type_strings() {
        use super::{haystack_type, JsonStringHaystackType};
        assert_eq!(haystack_type(""), JsonStringHaystackType::PlainString);
        assert_eq!(haystack_type(":"), JsonStringHaystackType::PlainString);
        assert_eq!(haystack_type("5"), JsonStringHaystackType::PlainString);
        assert_eq!(haystack_type("w:"), JsonStringHaystackType::PlainString);
        assert_eq!(haystack_type("hello"), JsonStringHaystackType::PlainString);

        assert_eq!(haystack_type("s:"), JsonStringHaystackType::PrefixedString);
        assert_eq!(
            haystack_type("s:hello"),
            JsonStringHaystackType::PrefixedString
        );
        assert_eq!(
            haystack_type("s:hello world"),
            JsonStringHaystackType::PrefixedString
        );
    }

    #[test]
    fn haystack_type_non_strings() {
        use super::{haystack_type, JsonStringHaystackType};
        assert_eq!(haystack_type("m:"), JsonStringHaystackType::Marker);
        assert_eq!(haystack_type("m:junk"), JsonStringHaystackType::Marker);
        assert_eq!(haystack_type("-:"), JsonStringHaystackType::RemoveMarker);
        assert_eq!(
            haystack_type("-:junk"),
            JsonStringHaystackType::RemoveMarker
        );

        assert_eq!(haystack_type("z:"), JsonStringHaystackType::Na);
        assert_eq!(haystack_type("z: junk"), JsonStringHaystackType::Na);

        assert_eq!(
            haystack_type("n:55 celsius"),
            JsonStringHaystackType::Number
        );
        assert_eq!(
            haystack_type("r:p:proj:r:abcd1234-abcd1234"),
            JsonStringHaystackType::Ref
        );
        assert_eq!(haystack_type("d:2014-01-03"), JsonStringHaystackType::Date);
        assert_eq!(haystack_type("h:23:59"), JsonStringHaystackType::Time);
        assert_eq!(
            haystack_type("t:2015-06-08T15:47:41-04:00 New_York"),
            JsonStringHaystackType::DateTime
        );
        assert_eq!(
            haystack_type("u:http://project-haystack.org/"),
            JsonStringHaystackType::Uri
        );
        assert_eq!(
            haystack_type("c:37.545,-77.449"),
            JsonStringHaystackType::Coord
        );
        assert_eq!(haystack_type("x:Type:value"), JsonStringHaystackType::XStr);
    }

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
