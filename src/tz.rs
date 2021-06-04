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
            if let Some(short_name) = full_name.split("/").skip(1).next() {
                // If the Tz name is in the format "RegionName/CityName":
                short_name == s.as_ref()
            } else {
                false
            }
        }
    });
    matching_tz.map(|tz| tz.clone())
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
