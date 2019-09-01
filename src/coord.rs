/// A Haystack Coord, representing a geographical
/// coordinate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Coord {
    lat: f64,
    lng: f64,
}

impl Coord {
    /// Create a new `Coord`.
    pub fn new(lat: f64, lng: f64) -> Self {
        Self { lat, lng }
    }

    /// Return the latitude component of this `Coord`.
    pub fn lat(&self) -> f64 {
        self.lat
    }

    /// Return the longitude component of this `Coord`.
    pub fn lng(&self) -> f64 {
        self.lng
    }
}
