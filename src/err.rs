use crate::auth::AuthError;
use crate::grid::{Grid, ParseJsonGridError};
use thiserror::Error;

impl Error {
    /// Return true if this error encapsulates a Haystack error grid.
    pub fn is_grid(&self) -> bool {
        match self {
            Self::Grid { .. } => true,
            _ => false,
        }
    }

    /// Return a reference to the Haystack error grid encapsulated by this
    /// error, if this error was caused by a Haystack error grid.
    pub fn grid(&self) -> Option<&Grid> {
        match self {
            Self::Grid { err_grid } => Some(err_grid),
            _ => None,
        }
    }

    /// Return the Haystack error grid encapsulated by this error, if this
    /// error was caused by a Haystack error grid.
    pub fn into_grid(self) -> Option<Grid> {
        match self {
            Self::Grid { err_grid } => Some(err_grid),
            _ => None,
        }
    }
}

/// Describes the kinds of errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The grid contained error information from the server.
    #[error("Server returned an error grid")]
    Grid {
        /// The grid which caused this error.
        err_grid: Grid,
    },
    /// An error which originated from the underlying HTTP library.
    #[error("Error occurred in the underlying HTTP library")]
    Http {
        #[from]
        err: reqwest::Error,
    },
    #[error("Could not parse JSON as a Haystack grid")]
    /// An error related to parsing a `Grid` from a JSON value.
    ParseJsonGrid(#[from] ParseJsonGridError),
}

/// Errors that can occur when creating a new `SkySparkClient`.
#[derive(Debug, Error)]
pub enum NewSkySparkClientError {
    /// An error which occurred during the authentication process.
    #[error("Error occurred during authentication")]
    Auth(#[from] AuthError),
    /// An error caused by an invalid SkySpark project url.
    #[error("The SkySpark URL is invalid: {msg}")]
    Url { msg: String },
}

impl NewSkySparkClientError {
    pub(crate) fn url(msg: &str) -> Self {
        NewSkySparkClientError::Url { msg: msg.into() }
    }
}

/// Errors that can occur when creating a new `ClientSeed`.
#[derive(Debug, Error)]
#[error("Could not create a new client seed")]
pub struct NewClientSeedError(#[from] reqwest::Error);
