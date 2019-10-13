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
    #[error("Error occurred in the underlying HTTP library: {err}")]
    Http {
        #[from]
        err: reqwest::Error
    },
    #[error("Could not parse JSON as a Haystack grid: {msg}")]
    /// An error related to parsing a `Grid`.
    ParseJsonGrid { msg: String },
}

impl From<ParseJsonGridError> for Error {
    fn from(error: ParseJsonGridError) -> Self {
        Error::ParseJsonGrid { msg: error.msg }
    }
}

/// Errors that can occur when creating a new `SkySparkClient`.
#[derive(Debug, Error)]
pub enum NewSkySparkClientError {
    /// An error which occurred during the authentication process.
    #[error("Error occurred during authentication: {0}")]
    Auth(#[from] AuthError),
    /// An error originating in the underlying HTTP client.
    #[error("Error occurred in the underlying HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
}