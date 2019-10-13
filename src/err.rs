// use crate::auth::AuthError;
use crate::grid::{Grid, ParseJsonGridError};

/// Encapsulates all errors that can occur in this crate.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    /// Return the `ErrorKind` for this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Return the `ErrorKind` for this error and consume the error.
    pub fn into_kind(self) -> ErrorKind {
        self.kind
    }

    /// Return true if this error encapsulates a Haystack error grid.
    pub fn is_grid(&self) -> bool {
        match self.kind() {
            ErrorKind::Grid { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn new(kind: ErrorKind) -> Self {
        Error { kind }
    }

    pub(crate) fn new_io(msg: String) -> Self {
        Error::new(ErrorKind::Io { msg })
    }

    /// Return the Haystack error grid encapsulated by this error, if this
    /// error was caused by a Haystack error grid.
    pub fn into_grid(self) -> Option<Grid> {
        match self.kind {
            ErrorKind::Grid { err_grid } => Some(err_grid),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self.kind() {
            // ErrorKind::Auth { err } => {
            //     format!("Error while authenticating: {}", err)
            // }
            ErrorKind::Csv { err } => format!("CSV error: {}", err),
            ErrorKind::Grid { err_grid } => {
                let trace = err_grid
                    .error_trace()
                    .unwrap_or_else(|| "No error trace".to_owned());
                format!("Error grid: {}", trace)
            }
            ErrorKind::Http { err } => format!("HTTP error: {}", err),
            ErrorKind::Io { msg } => format!("IO error: {}", msg),
            ErrorKind::ParseJsonGrid { msg } => {
                format!("Could not parse a grid from JSON: {}", msg)
            }
        };
        write!(f, "Error - {}", msg)
    }
}

/// Describes the kinds of errors that can occur in this crate.
#[derive(Debug)]
pub enum ErrorKind {
    // /// An error which occurred during the authorization process.
    // Auth { err: AuthError },
    /// An error related to CSVs.
    Csv { err: csv::Error },
    /// The grid contained error information from the server.
    Grid {
        /// The grid which caused this error.
        err_grid: Grid,
    },
    /// An error which originated from the underlying HTTP library.
    Http { err: reqwest::Error },
    /// An IO error.
    Io { msg: String },
    /// An error related to parsing a `Grid`.
    ParseJsonGrid { msg: String },
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind() {
            // ErrorKind::Auth { err } => Some(err),
            ErrorKind::Csv { err } => Some(err),
            ErrorKind::Grid { .. } => None,
            ErrorKind::Http { err } => Some(err),
            ErrorKind::Io { .. } => None,
            ErrorKind::ParseJsonGrid { .. } => None,
        }
    }
}

// impl From<AuthError> for Error {
//     fn from(err: AuthError) -> Self {
//         Error::new(ErrorKind::Auth { err })
//     }
// }

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Error::new(ErrorKind::Http { err: error })
    }
}

impl From<ParseJsonGridError> for Error {
    fn from(error: ParseJsonGridError) -> Self {
        Error::new(ErrorKind::ParseJsonGrid { msg: error.msg })
    }
}

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Self {
        Error::new(ErrorKind::Csv { err: error })
    }
}
