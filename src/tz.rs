use chrono_tz::{Tz, TZ_VARIANTS};

/// Converts a string containing a SkySpark time zone name into the matching
/// `Tz` variant from the chrono_tz crate.
pub fn skyspark_tz_string_to_tz<T>(s: T) -> Option<Tz>
where
    T: AsRef<str>,
{
    let matching_tz = TZ_VARIANTS.iter().find(|tz| {
        let full_name = tz.name();
        let is_full_name_match = full_name == s.as_ref();

        if is_full_name_match {
            true
        } else {
            let short_name = time_zone_name_to_short_name(full_name);
            short_name == s.as_ref()
        }
    });
    matching_tz.map(|tz| tz.clone())
}

/// Given an IANA TZDB identifier like  "America/New_York", return the
/// short time zone name used by SkySpark (like "New_York").
pub(crate) fn time_zone_name_to_short_name(tz_name: &str) -> &str {
    let parts: Vec<_> =
        tz_name.split("/").filter(|s| !s.is_empty()).collect();
    parts.last().expect("time zone parts should not be empty")
}

#[cfg(test)]
mod test {
    use super::skyspark_tz_string_to_tz;

    #[test]
    fn short_name_match_works() {
        let tz = skyspark_tz_string_to_tz("Sydney").unwrap();
        assert_eq!(tz, chrono_tz::Tz::Australia__Sydney);
    }

    #[test]
    fn full_name_match_works() {
        let tz = skyspark_tz_string_to_tz("Australia/Sydney").unwrap();
        assert_eq!(tz, chrono_tz::Tz::Australia__Sydney);
    }
}
