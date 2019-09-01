#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Coord {
    lat: f64,
    lng: f64,
}

impl Coord {
    pub fn new(lat: f64, lng: f64) -> Self {
        Self { lat, lng }
    }

    pub fn lat(&self) -> f64 {
        self.lat
    }

    pub fn lng(&self) -> f64 {
        self.lng
    }
}