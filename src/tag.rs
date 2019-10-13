use thiserror::Error;

/// A Haystack tag name.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TagName(String);

impl TagName {
    /// Create a new `TagName`
    pub fn new(s: String) -> Option<Self> {
        if is_tag_name(&s) {
            Some(TagName(s))
        } else {
            None
        }
    }

    /// Return the tag name string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for TagName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Return true if the string is a valid tag name.
pub fn is_tag_name<T: AsRef<str>>(s: T) -> bool {
    let s = s.as_ref();
    if s.is_empty() {
        false
    } else {
        let chars = s.chars().enumerate();
        let mut is_tag_name = true;
        for (index, c) in chars {
            if index == 0 {
                if !c.is_ascii_lowercase() {
                    is_tag_name = false;
                    break;
                }
            } else if !(c.is_ascii_alphanumeric() || c == '_') {
                is_tag_name = false;
                break;
            };
        }
        is_tag_name
    }
}

impl std::str::FromStr for TagName {
    type Err = ParseTagNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if is_tag_name(s) {
            Ok(TagName(s.to_owned()))
        } else {
            let unparsable_tag_name = s.to_owned();
            Err(ParseTagNameError {
                unparsable_tag_name,
            })
        }
    }
}

impl std::convert::AsRef<str> for TagName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::convert::AsRef<[u8]> for TagName {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// An error indicating that a `TagName` could not be parsed.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("Could not parse a tag name from the string {unparsable_tag_name}")]
pub struct ParseTagNameError {
    unparsable_tag_name: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn is_tag_name() {
        use super::is_tag_name;
        assert_eq!(is_tag_name("siteRef"), true);
        assert_eq!(is_tag_name("s"), true);
        assert_eq!(is_tag_name("s1"), true);
        assert_eq!(is_tag_name(""), false);
        assert_eq!(is_tag_name("1s"), false);
        assert_eq!(is_tag_name("s%"), false);
        assert_eq!(is_tag_name("s-"), false);
        assert_eq!(is_tag_name("s_s"), true);
        assert_eq!(is_tag_name("_s"), false);
        assert_eq!(is_tag_name("s_"), true);
    }
}
