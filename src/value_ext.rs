use crate::{Date, DateTime, Time};
use raystack_core::{
    Coord, Hayson, Marker, Na, Number, Ref, RemoveMarker, Symbol, Uri, Xstr,
};
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
    /// Convert the JSON value to a Haystack DateTime.
    fn as_hs_date_time(&self) -> Option<DateTime>;
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

    fn as_hs_date_time(&self) -> Option<DateTime> {
        DateTime::from_hayson(self).ok()
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
