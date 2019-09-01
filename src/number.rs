#[derive(Clone, Debug, PartialEq)]
pub struct Number {
    value: f64,
    unit: Option<String>,
}

impl Number {
    pub fn new(value: f64, unit: Option<String>) -> Self {
        Self { value, unit }
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn unit(&self) -> Option<&str> {
        self.unit.as_ref().map(|unit| unit.as_ref())
    }

    pub fn from_encoded_json_string(
        json_string: &str,
    ) -> Result<Self, ParseNumberError> {
        let json_string = json_string.replacen("n:", "", 1);
        let mut split = json_string.trim().split(' ');
        let number_str = split.next();
        let unit_str = split.next();

        if let Some(number_str) = number_str {
            let number: f64 = number_str
                .parse()
                .map_err(|_| ParseNumberError::from_str(&json_string))?;
            let unit = unit_str.map(|unit_str| unit_str.trim().to_string());
            Ok(Number::new(number, unit))
        } else {
            Err(ParseNumberError::from_str(&json_string))
        }
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
}
