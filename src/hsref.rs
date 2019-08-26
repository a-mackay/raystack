/// A Haystack Ref.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ref(String);

impl Ref {
    /// Create a new `Ref`.
    ///
    /// # Example
    /// ```rust
    /// use raystack::Ref;
    /// let my_ref = Ref::new("@p:bigProject:r:24efe1c4-24aef280".to_string()).unwrap();
    /// ```
    pub fn new(s: String) -> Result<Self, ParseRefError> {
        if Self::is_valid_ref(&s) {
            Ok(Ref(s))
        } else {
            Err(ParseRefError::from_string(s))
        }
    }
    /// Return a Ref by decoding a ref which was encoded in a JSON string. In
    /// raw JSON strings, ref are formatted with a `r:` prefix instead of
    /// an `@` sign.
    ///
    /// # Example
    /// ```rust
    /// use raystack::Ref;
    /// let json_str = "r:p:bigProject:r:24efe1c4-24aef280";
    /// let my_ref = Ref::from_encoded_json_string(json_str).unwrap();
    /// ```
    pub fn from_encoded_json_string(
        json_string: &str,
    ) -> Result<Self, ParseRefError> {
        if let Some(raw_id) = json_string.split(' ').next() {
            Self::new(raw_id.replacen("r:", "@", 1))
        } else {
            Err(ParseRefError::from_str(json_string))
        }
    }

    /// Return a string containing this ref, encoded with a `r:` prefix instead
    /// of with an `@` sign. This representation for refs is used in raw
    /// JSON strings sent to and from a Haystack server.
    pub fn to_encoded_json_string(&self) -> String {
        self.0.replacen("@", "r:", 1)
    }

    /// Convert this ref into a string.
    pub fn into_string(self) -> String {
        self.0
    }

    /// Return this ref as an Axon ref literal.
    pub fn to_axon_code(&self) -> &str {
        self.as_ref()
    }

    /// Return true if the string can be parsed as a valid ref.
    pub(crate) fn is_valid_ref(s: &str) -> bool {
        if s.is_empty() {
            false
        } else {
            let chars = s.chars().enumerate();

            let mut is_valid_ref = true;
            let mut last_index_seen = 0;

            for (index, c) in chars {
                if index == 0 {
                    if c != '@' {
                        is_valid_ref = false;
                        break;
                    }
                } else {
                    last_index_seen = index;
                    if !(Self::is_valid_ref_char(c)) {
                        is_valid_ref = false;
                        break;
                    }
                };
            }

            if last_index_seen == 0 {
                false
            } else {
                is_valid_ref
            }
        }
    }

    fn is_valid_ref_char(c: char) -> bool {
        c.is_alphanumeric() || Self::is_valid_symbol_char(c)
    }

    fn is_valid_symbol_char(c: char) -> bool {
        c == '_' || c == ':' || c == '-' || c == '.' || c == '~'
    }
}

impl std::str::FromStr for Ref {
    type Err = ParseRefError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if Self::is_valid_ref(s) {
            Ok(Ref(s.to_owned()))
        } else {
            let unparsable_ref = s.to_owned();
            Err(ParseRefError { unparsable_ref })
        }
    }
}

impl std::convert::AsRef<str> for Ref {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// An error indicating that a `Ref` could not be parsed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseRefError {
    unparsable_ref: String,
}

impl ParseRefError {
    pub(crate) fn from_str(s: &str) -> Self {
        let unparsable_ref = s.to_owned();
        ParseRefError { unparsable_ref }
    }

    pub(crate) fn from_string(s: String) -> Self {
        ParseRefError { unparsable_ref: s }
    }
}

impl std::fmt::Display for ParseRefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not parse Ref from string {}", self.unparsable_ref)
    }
}

impl std::error::Error for ParseRefError {}

#[cfg(test)]
mod test {
    use super::Ref;
    #[test]
    fn parse_ref() {
        assert_eq!(Ref::is_valid_ref("@p:some_proj:r:1e85e02f-0459cf96"), true);
        assert_eq!(Ref::is_valid_ref("@H.NAE_05.NAE~2d05~2fFC~2d2~2eFD~2d21-VAV~2d10~2d17~2eVAV~2d10~2d17-ZNT~2dSP~2eTrend1"), true);
        assert_eq!(Ref::is_valid_ref("@"), false);
        assert_eq!(Ref::is_valid_ref(""), false);
        assert_eq!(Ref::is_valid_ref("@o/o"), false);
        assert_eq!(Ref::is_valid_ref("@o,o"), false);
        assert_eq!(Ref::is_valid_ref("@o|o"), false);
    }
}
