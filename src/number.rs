/// A Haystack Number, encapsulating a scalar value and
/// an optional unit value. The unit is represented as a
/// string.
#[derive(Clone, Debug, PartialEq)]
pub struct Number {
    value: f64,
    unit: Option<String>,
}

impl Number {
    /// Create a new `Number`. If present, the unit should
    /// be a valid unit string from Project Haystack's
    /// unit database.
    pub fn new(value: f64, unit: Option<String>) -> Self {
        Self { value, unit }
    }

    /// Return the numeric component of this `Number`.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Return the unit component of this `Number`, if present.
    pub fn unit(&self) -> Option<&str> {
        self.unit.as_ref().map(|unit| unit.as_ref())
    }

    /// Parse a `Number` from a number encoded in a JSON string.
    /// # Example
    /// ```rust
    /// use raystack::Number;
    ///
    /// let n = Number::new(1.0, Some("pH".to_owned()));
    /// assert_eq!(Number::from_encoded_json_string("n:1.0 pH").unwrap(), n);
    /// ```
    pub fn from_encoded_json_string(
        json_string: &str,
    ) -> Result<Self, ParseNumberError> {
        let json_string = json_string.replacen("n:", "", 1);
        let mut split = json_string.trim().split(' ');
        let number_str = split.next();
        let unit_str = split.next();

        if let Some(number_str) = number_str {
            let number = if number_str == "INF" {
                std::f64::INFINITY
            } else if number_str == "-INF" {
                std::f64::NEG_INFINITY
            } else {
                number_str
                    .parse()
                    .map_err(|_| ParseNumberError::from_str(&json_string))?
            };
            let unit = unit_str.map(|unit_str| unit_str.trim().to_string());
            Ok(Number::new(number, unit))
        } else {
            Err(ParseNumberError::from_str(&json_string))
        }
    }

    /// Return a string representation of this number.
    pub fn to_string(&self) -> String {
        if let Some(unit) = self.unit() {
            format!("{} {}", self.value(), unit)
        } else {
            self.value().to_string()
        }
    }
}

impl std::fmt::Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// An error indicating that a `Number` could not be parsed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseNumberError {
    unparsable_number: String,
}

impl ParseNumberError {
    pub(crate) fn from_str(s: &str) -> Self {
        Self {
            unparsable_number: s.to_string(),
        }
    }
}

impl std::fmt::Display for ParseNumberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Could not parse Number from string {}",
            self.unparsable_number
        )
    }
}

impl std::error::Error for ParseNumberError {}

#[cfg(test)]
mod test {
    use super::Number;

    #[test]
    fn from_encoded_json_string() {
        let unitless = "n:45.5";
        assert_eq!(
            Number::from_encoded_json_string(unitless).unwrap().value(),
            45.5
        );

        let unit = "n:73.2 °F";
        let number_with_unit = Number::from_encoded_json_string(unit).unwrap();
        assert_eq!(number_with_unit.value(), 73.2);
        assert_eq!(number_with_unit.unit(), Some("°F"))
    }

    #[test]
    fn from_encoded_json_string_infinity() {
        let unitless = "n:INF";
        assert_eq!(
            Number::from_encoded_json_string(unitless).unwrap().value(),
            std::f64::INFINITY,
        );

        let unit = "n:INF °F";
        let number_with_unit = Number::from_encoded_json_string(unit).unwrap();
        assert_eq!(number_with_unit.value(), std::f64::INFINITY);
        assert_eq!(number_with_unit.unit(), Some("°F"))
    }

    #[test]
    fn from_encoded_json_string_neg_infinity() {
        let unitless = "n:-INF";
        assert_eq!(
            Number::from_encoded_json_string(unitless).unwrap().value(),
            std::f64::NEG_INFINITY,
        );

        let unit = "n:-INF °F";
        let number_with_unit = Number::from_encoded_json_string(unit).unwrap();
        assert_eq!(number_with_unit.value(), std::f64::NEG_INFINITY);
        assert_eq!(number_with_unit.unit(), Some("°F"))
    }

    #[test]
    fn from_encoded_json_string_signless_nan() {
        let unitless = "n:NaN";
        assert!(Number::from_encoded_json_string(unitless)
            .unwrap()
            .value()
            .is_nan());

        let unit = "n:NaN °F";
        let number_with_unit = Number::from_encoded_json_string(unit).unwrap();
        assert!(number_with_unit.value().is_nan());
        assert_eq!(number_with_unit.unit(), Some("°F"))
    }

    #[test]
    fn from_encoded_json_string_signed_nan() {
        let unitless = "n:-NaN";
        assert!(Number::from_encoded_json_string(unitless)
            .unwrap()
            .value()
            .is_nan());

        let unit = "n:+NaN °F";
        let number_with_unit = Number::from_encoded_json_string(unit).unwrap();
        assert!(number_with_unit.value().is_nan());
        assert_eq!(number_with_unit.unit(), Some("°F"))
    }
}
