use crate::{
    add_backslash_if_necessary, has_valid_path_segments, http_response_to_grid,
    new_auth_token,
};
use crate::{ClientSeed, Grid};
use serde_json::json;
use thiserror::Error;
use url::Url;

/// A standalone function to call the eval API on a SkySpark server, without
/// creating a `SkySparkClient`.
///
/// # Example
/// ```rust,no_run
/// # async fn run() {
/// use raystack::ClientSeed;
/// use raystack::eval::eval;
/// let client_seed = ClientSeed::new(30).unwrap();
/// let url = "http://test.com/api/bigProject/";
/// let output = eval(&client_seed, url, "name", "p4ssw0rd", "readAll(site)", None).await.unwrap();
/// let grid = output.into_grid();
/// // Use the grid here
/// # }
/// ```
pub async fn eval(
    client_seed: &ClientSeed,
    project_api_url: &str,
    username: &str,
    password: &str,
    axon_expr: &str,
    auth_token: Option<&str>,
) -> Result<EvalOutput, EvalError> {
    let project_api_url = Url::parse(project_api_url)?;
    let project_api_url = add_backslash_if_necessary(project_api_url);

    if project_api_url.cannot_be_a_base() {
        let url_err_msg = "the project API URL must be a valid base URL";
        return Err(EvalError::UrlFormat(url_err_msg.to_owned()));
    }

    if !has_valid_path_segments(&project_api_url) {
        let url_err_msg = "URL must be formatted similarly to http://www.test.com/api/project/";
        return Err(EvalError::UrlFormat(url_err_msg.to_owned()));
    }

    let eval_url = project_api_url
        .join("eval")
        .expect("since url ends with '/' this should never fail");

    let mut was_new_token_obtained = false;

    let auth_token = match auth_token {
        Some(token) => token.to_owned(),
        None => {
            was_new_token_obtained = true;
            new_auth_token(&project_api_url, &client_seed, username, password)
                .await?
        }
    };

    let row = json!({ "expr": axon_expr });
    let req_grid = Grid::new_internal(vec![row]);

    let req_with_token = |token: &str| {
        client_seed
            .client()
            .post(eval_url.clone())
            .header("Accept", "application/json")
            .header("Authorization", format!("BEARER authToken={}", token))
            .header("Content-Type", "application/json")
            .body(req_grid.to_json_string())
    };

    let res = req_with_token(&auth_token).send().await?;

    if res.status() == reqwest::StatusCode::FORBIDDEN {
        let auth_token =
            new_auth_token(&project_api_url, &client_seed, username, password)
                .await?;
        let retry_res = req_with_token(&auth_token).send().await?;
        let grid: Result<Grid, EvalError> = http_response_to_grid(retry_res)
            .await
            .map_err(|err| err.into());
        Ok(EvalOutput::new(grid?, Some(auth_token)))
    } else {
        let grid: Result<Grid, EvalError> =
            http_response_to_grid(res).await.map_err(|err| err.into());
        if was_new_token_obtained {
            Ok(EvalOutput::new(grid?, Some(auth_token)))
        } else {
            Ok(EvalOutput::new(grid?, None))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// The resulting output of a call to the SkySpark eval API.
pub struct EvalOutput {
    /// The grid returned from the server.
    grid: Grid,
    /// If a new auth token was obtained while executing the eval function,
    /// this will contain that auth token.
    new_auth_token: Option<String>,
}

impl EvalOutput {
    fn new(grid: Grid, new_auth_token: Option<String>) -> Self {
        Self {
            grid,
            new_auth_token,
        }
    }

    pub fn grid(&self) -> &Grid {
        &self.grid
    }

    pub fn into_grid(self) -> Grid {
        self.grid
    }

    /// Return true only if a new auth token was obtained.
    pub fn has_new_auth_token(&self) -> bool {
        self.new_auth_token.is_some()
    }

    /// If a new auth token was obtained, return that token.
    pub fn new_auth_token(&self) -> Option<&str> {
        match &self.new_auth_token {
            Some(token) => Some(token),
            None => None,
        }
    }
}

/// Errors that can occur when executing an eval API call on
/// a SkySpark server.
#[derive(Debug, Error)]
pub enum EvalError {
    #[error("Could not parse a URL: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("URL is not formatted for the SkySpark API: {0}")]
    UrlFormat(String),
    #[error("Authentication error: {0}")]
    Auth(#[from] crate::auth::AuthError),
    /// The grid contained error information from the server.
    #[error("Server returned an error grid")]
    Grid {
        /// The grid which caused this error.
        err_grid: Grid,
    },
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// An error related to parsing a `Grid` from a JSON value.
    #[error("Could not parse JSON as a Haystack grid")]
    ParseJsonGrid(#[from] crate::grid::ParseJsonGridError),
    /// An error caused by an invalid time zone.
    #[error("Not a valid time zone: {err_time_zone}")]
    TimeZone {
        /// The time zone which caused this error.
        err_time_zone: String,
    },
}

impl std::convert::From<crate::Error> for EvalError {
    fn from(error: crate::Error) -> Self {
        match error {
            crate::Error::Grid { err_grid } => Self::Grid { err_grid },
            crate::Error::Http { err } => Self::Http(err),
            crate::Error::ParseJsonGrid(err) => Self::ParseJsonGrid(err),
            crate::Error::TimeZone { err_time_zone } => {
                Self::TimeZone { err_time_zone }
            }
            crate::Error::UpdateAuthToken(_) => unreachable!(), // The standalone eval function will not update auth tokens.
        }
    }
}

impl EvalError {
    /// Return true if this error encapsulates a Haystack error grid.
    pub fn is_grid(&self) -> bool {
        matches!(self, Self::Grid { .. })
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

#[cfg(test)]
mod test {
    use super::eval;
    use super::{EvalError, EvalOutput};
    use crate::{ClientSeed, ValueExt};

    fn project_api_url() -> String {
        std::env::var("RAYSTACK_SKYSPARK_PROJECT_API_URL").unwrap()
    }

    fn username() -> String {
        std::env::var("RAYSTACK_SKYSPARK_USERNAME").unwrap()
    }

    fn password() -> String {
        std::env::var("RAYSTACK_SKYSPARK_PASSWORD").unwrap()
    }

    async fn eval_expr(
        axon_expr: &str,
        token: Option<&str>,
    ) -> Result<EvalOutput, EvalError> {
        let seed = ClientSeed::new(15).unwrap();

        eval(
            &seed,
            &project_api_url(),
            &username(),
            &password(),
            axon_expr,
            token,
        )
        .await
    }

    #[tokio::test]
    async fn eval_works_with_no_token() {
        let output = eval_expr("readAll(site)", None).await.unwrap();
        assert!(output.has_new_auth_token());
        let grid = output.into_grid();
        assert!(grid.size() > 1);
        assert!(grid.rows()[0]["site"].is_hs_marker());
    }

    #[tokio::test]
    async fn eval_works_with_bad_token() {
        let output = eval_expr("readAll(site)", Some("thistokenisnotvalid"))
            .await
            .unwrap();
        assert!(output.has_new_auth_token());
        let grid = output.into_grid();
        assert!(grid.size() > 1);
        assert!(grid.rows()[0]["site"].is_hs_marker());
    }

    #[tokio::test]
    async fn eval_works_with_good_token() {
        let output_for_token = eval_expr("readAll(site)", None).await.unwrap();
        let valid_token = output_for_token.new_auth_token().unwrap();
        let output =
            eval_expr("readAll(site)", Some(valid_token)).await.unwrap();

        // We used a valid token, so there should be no new token:
        assert_eq!(output.has_new_auth_token(), false);

        let grid = output.into_grid();
        assert!(grid.size() > 1);
        assert!(grid.rows()[0]["site"].is_hs_marker());
    }
}
