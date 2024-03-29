//! # Overview
//! This crate provides functions which can query a SkySpark server, using
//! the Haystack REST API and the SkySpark REST API's `eval` operation.
//! Some Haystack operations are not implemented
//! (watch* operations, pointWrite and invokeAction).
//!
//! # Example Usage
//! Put this in the main function in the `main.rs` file to create
//! and use a `SkySparkClient`:
//!
//! ```rust,no_run
//! # async fn run() {
//! use raystack::{SkySparkClient, ValueExt};
//! use url::Url;
//!
//! let url = Url::parse("https://www.example.com/api/projName/").unwrap();
//! let mut client = SkySparkClient::new(url, "username", "p4ssw0rd").await.unwrap();
//! let sites_grid = client.eval("readAll(site)").await.unwrap();
//!
//! // Print the raw JSON:
//! println!("{}", sites_grid.to_json_string_pretty());
//!
//! // Working with the Grid struct:
//! println!("All columns: {:?}", sites_grid.cols());
//! println!("first site id: {:?}", sites_grid.rows()[0]["id"].as_hs_ref().unwrap());
//! # }
//! ```
//!
//! See the `examples` folder for more usage examples.
//!
//! The Grid struct is a wrapper around the underlying JSON Value enum
//! provided by the `serde_json` crate. See the
//! [documentation for Value](https://docs.serde.rs/serde_json/enum.Value.html)
//! for more information on how to query for data stored within it.
//!
//! Additional functions for extracting Haystack values from the underlying
//! JSON are found in this crate's `ValueExt` trait.

mod api;
pub mod auth;
mod err;
pub mod eval;
mod grid;
mod hs_types;
mod tz;
mod value_ext;

use api::HaystackUrl;
pub use api::HisReadRange;
use chrono::Utc;
pub use err::{Error, NewSkySparkClientError};
pub use grid::{Grid, ParseJsonGridError};
pub use hs_types::{Date, DateTime, Time};
pub use raystack_core::Coord;
pub use raystack_core::{is_tag_name, ParseTagNameError, TagName};
pub use raystack_core::{BasicNumber, Number, ScientificNumber};
pub use raystack_core::{FromHaysonError, Hayson};
pub use raystack_core::{Marker, Na, RemoveMarker, Symbol, Uri, Xstr};
pub use raystack_core::{ParseRefError, Ref};
use serde_json::json;
use std::convert::TryInto;
pub use tz::skyspark_tz_string_to_tz;
use url::Url;
pub use value_ext::ValueExt;

type Result<T> = std::result::Result<T, Error>;
type StdResult<T, E> = std::result::Result<T, E>;

pub(crate) async fn new_auth_token(
    project_api_url: &Url,
    reqwest_client: &reqwest::Client,
    username: &str,
    password: &str,
) -> StdResult<String, crate::auth::AuthError> {
    let mut auth_url = project_api_url.clone();
    auth_url.set_path("/ui");

    let auth_token = auth::new_auth_token(
        reqwest_client,
        auth_url.as_str(),
        username,
        password,
    )
    .await?;

    Ok(auth_token)
}

/// A client for interacting with a SkySpark server.
#[derive(Debug)]
pub struct SkySparkClient {
    auth_token: String,
    client: reqwest::Client,
    username: String,
    password: String,
    project_api_url: Url,
}

impl SkySparkClient {
    /// Create a new `SkySparkClient`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn run() {
    /// use raystack::SkySparkClient;
    /// use url::Url;
    /// let url = Url::parse("https://skyspark.company.com/api/bigProject/").unwrap();
    /// let mut client = SkySparkClient::new(url, "username", "p4ssw0rd").await.unwrap();
    /// # }
    /// ```
    pub async fn new(
        project_api_url: Url,
        username: &str,
        password: &str,
    ) -> std::result::Result<Self, NewSkySparkClientError> {
        let client = reqwest::Client::new();
        Self::new_with_client(project_api_url, username, password, client).await
    }

    /// Create a new `SkySparkClient`, passing in an existing
    /// `reqwest::Client`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn run() {
    /// use raystack::SkySparkClient;
    /// use reqwest::Client;
    /// use url::Url;
    /// let reqwest_client = Client::new();
    /// let url = Url::parse("https://skyspark.company.com/api/bigProject/").unwrap();
    /// let mut client = SkySparkClient::new_with_client(url, "username", "p4ssw0rd", reqwest_client).await.unwrap();
    /// # }
    /// ```
    ///
    /// If creating multiple `SkySparkClient`s,
    /// the same `reqwest::Client` should be used for each. For example:
    ///
    /// ```rust,no_run
    /// # async fn run() {
    /// use raystack::SkySparkClient;
    /// use reqwest::Client;
    /// use url::Url;
    /// let reqwest_client = Client::new();
    /// let url1 = Url::parse("http://test.com/api/bigProject/").unwrap();
    /// let client1 = SkySparkClient::new_with_client(url1, "name", "password", reqwest_client.clone()).await.unwrap();
    /// let url2 = Url::parse("http://test.com/api/smallProj/").unwrap();
    /// let client2 = SkySparkClient::new_with_client(url2, "name", "password", reqwest_client.clone()).await.unwrap();
    /// # }
    /// ```
    pub async fn new_with_client(
        project_api_url: Url,
        username: &str,
        password: &str,
        reqwest_client: reqwest::Client,
    ) -> std::result::Result<Self, NewSkySparkClientError> {
        let project_api_url = add_backslash_if_necessary(project_api_url);

        if project_api_url.cannot_be_a_base() {
            let url_err_msg = "the project API URL must be a valid base URL";
            return Err(NewSkySparkClientError::url(url_err_msg));
        }

        if !has_valid_path_segments(&project_api_url) {
            let url_err_msg = "URL must be formatted similarly to http://www.test.com/api/project/";
            return Err(NewSkySparkClientError::url(url_err_msg));
        }

        Ok(SkySparkClient {
            auth_token: new_auth_token(
                &project_api_url,
                &reqwest_client,
                username,
                password,
            )
            .await?,
            client: reqwest_client,
            username: username.to_owned(),
            password: password.to_owned(),
            project_api_url,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_manually_set_auth_token(&mut self, auth_token: &str) {
        self.auth_token = auth_token.to_owned();
    }

    #[cfg(test)]
    pub(crate) fn test_auth_token(&self) -> &str {
        &self.auth_token
    }

    async fn update_auth_token(
        &mut self,
    ) -> StdResult<(), crate::auth::AuthError> {
        let auth_token = new_auth_token(
            self.project_api_url(),
            self.client(),
            &self.username,
            &self.password,
        )
        .await?;
        self.auth_token = auth_token;
        Ok(())
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    fn auth_header_value(&self) -> String {
        format!("BEARER authToken={}", self.auth_token)
    }

    fn eval_url(&self) -> Url {
        self.append_to_url("eval")
    }

    async fn get(&mut self, url: Url) -> Result<Grid> {
        let res = self.get_response(url.clone()).await?;

        if res.status() == reqwest::StatusCode::FORBIDDEN {
            self.update_auth_token().await?;
            let retry_res = self.get_response(url).await?;
            http_response_to_grid(retry_res).await
        } else {
            http_response_to_grid(res).await
        }
    }

    async fn get_response(&self, url: Url) -> Result<reqwest::Response> {
        self.client()
            .get(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .send()
            .await
            .map_err(|err| err.into())
    }

    async fn post(&mut self, url: Url, grid: &Grid) -> Result<Grid> {
        let res = self.post_response(url.clone(), grid).await?;

        if res.status() == reqwest::StatusCode::FORBIDDEN {
            self.update_auth_token().await?;
            let retry_res = self.post_response(url, grid).await?;
            http_response_to_grid(retry_res).await
        } else {
            http_response_to_grid(res).await
        }
    }

    async fn post_response(
        &self,
        url: Url,
        grid: &Grid,
    ) -> Result<reqwest::Response> {
        self.client()
            .post(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .header("Content-Type", "application/json")
            .body(grid.to_json_string())
            .send()
            .await
            .map_err(|err| err.into())
    }

    fn append_to_url(&self, s: &str) -> Url {
        self.project_api_url
            .join(s)
            .expect("since url ends with '/' this should never fail")
    }

    /// Return the project name for this client.
    pub fn project_name(&self) -> &str {
        // Since the URL is validated by the `SkySparkClient::new` function,
        // the following code shouldn't panic:
        self.project_api_url
            .path_segments()
            .expect("proj api url is a valid base URL so this shouldn't fail")
            .nth(1)
            .expect("since URL is valid, the project name should be present")
    }

    /// Return the project API url being used by this client.
    pub fn project_api_url(&self) -> &Url {
        &self.project_api_url
    }
}

/// If the given url ends with a backslash, return the url without
/// any modifications. If the given url does not end with a backslash,
/// append a backslash to the end and return a new `Url`.
pub(crate) fn add_backslash_if_necessary(url: Url) -> Url {
    let chars = url.as_str().chars().collect::<Vec<_>>();
    let last_char = chars.last().expect("parsed url should have >= 1 chars");
    if *last_char != '/' {
        Url::parse(&(url.to_string() + "/")).expect("adding '/' to the end of a parsable url should create another parsable url")
    } else {
        url
    }
}

impl SkySparkClient {
    /// Returns a grid containing basic server information.
    pub async fn about(&mut self) -> Result<Grid> {
        self.get(self.about_url()).await
    }

    /// Returns a grid describing what MIME types are available.
    pub async fn filetypes(&mut self) -> Result<Grid> {
        self.get(self.filetypes_url()).await
    }

    /// Returns a grid of history data for a single point.
    pub async fn his_read(
        &mut self,
        id: &Ref,
        range: &HisReadRange,
    ) -> Result<Grid> {
        let row = json!({
            "id": id.to_hayson(),
            "range": range.to_json_request_string()
        });
        let req_grid = Grid::new_internal(vec![row]);

        self.post(self.his_read_url(), &req_grid).await
    }

    /// Writes boolean values to a single point.
    pub async fn his_write_bool(
        &mut self,
        id: &Ref,
        his_data: &[(DateTime, bool)],
    ) -> Result<Grid> {
        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                json!({
                    "ts": date_time.to_hayson(),
                    "val": value
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    /// Writes numeric values to a single point. `unit` must be a valid
    /// Haystack unit literal, such as `L/s` or `celsius`.
    pub async fn his_write_num(
        &mut self,
        id: &Ref,
        his_data: &[(DateTime, Number)],
    ) -> Result<Grid> {
        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                json!({
                    "ts": date_time.to_hayson(),
                    "val": value.to_hayson(),
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    /// Writes string values to a single point.
    pub async fn his_write_str(
        &mut self,
        id: &Ref,
        his_data: &[(DateTime, String)],
    ) -> Result<Grid> {
        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                json!({
                    "ts": date_time.to_hayson(),
                    "val": value
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    /// Writes boolean values with UTC timestamps to a single point.
    /// `time_zone_name` must be a valid SkySpark timezone name.
    pub async fn utc_his_write_bool(
        &mut self,
        id: &Ref,
        time_zone_name: &str,
        his_data: &[(chrono::DateTime<Utc>, bool)],
    ) -> Result<Grid> {
        let tz = skyspark_tz_string_to_tz(time_zone_name).ok_or_else(|| {
            Error::TimeZone {
                err_time_zone: time_zone_name.to_owned(),
            }
        })?;

        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                let date_time: DateTime = date_time.with_timezone(&tz).into();
                json!({
                    "ts": date_time.to_hayson(),
                    "val": value
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    /// Writes numeric values with UTC timestamps to a single point.
    /// `unit` must be a valid Haystack unit literal, such as `L/s` or
    /// `celsius`.
    /// `time_zone_name` must be a valid SkySpark timezone name.
    pub async fn utc_his_write_num(
        &mut self,
        id: &Ref,
        time_zone_name: &str,
        his_data: &[(chrono::DateTime<Utc>, Number)],
    ) -> Result<Grid> {
        let tz = skyspark_tz_string_to_tz(time_zone_name).ok_or_else(|| {
            Error::TimeZone {
                err_time_zone: time_zone_name.to_owned(),
            }
        })?;

        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                let date_time: DateTime = date_time.with_timezone(&tz).into();

                json!({
                    "ts": date_time.to_hayson(),
                    "val": value.to_hayson(),
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    /// Writes string values with UTC timestamps to a single point.
    /// `time_zone_name` must be a valid SkySpark timezone name.
    pub async fn utc_his_write_str(
        &mut self,
        id: &Ref,
        time_zone_name: &str,
        his_data: &[(chrono::DateTime<Utc>, String)],
    ) -> Result<Grid> {
        let tz = skyspark_tz_string_to_tz(time_zone_name).ok_or_else(|| {
            Error::TimeZone {
                err_time_zone: time_zone_name.to_owned(),
            }
        })?;

        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                let date_time: DateTime = date_time.with_timezone(&tz).into();

                json!({
                    "ts": date_time.to_hayson(),
                    "val": value,
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    /// The Haystack nav operation.
    pub async fn nav(&mut self, nav_id: Option<&Ref>) -> Result<Grid> {
        let req_grid = match nav_id {
            Some(nav_id) => {
                let row = json!({ "navId": nav_id.to_hayson() });
                Grid::new_internal(vec![row])
            }
            None => Grid::new_internal(Vec::new()),
        };

        self.post(self.nav_url(), &req_grid).await
    }

    /// Returns a grid containing the operations available on the server.
    pub async fn ops(&mut self) -> Result<Grid> {
        self.get(self.ops_url()).await
    }

    /// Returns a grid containing the records matching the given Axon
    /// filter string.
    pub async fn read(
        &mut self,
        filter: &str,
        limit: Option<u64>,
    ) -> Result<Grid> {
        let row = match limit {
            Some(integer) => json!({"filter": filter, "limit": integer}),
            None => json!({ "filter": filter }),
        };

        let req_grid = Grid::new_internal(vec![row]);
        self.post(self.read_url(), &req_grid).await
    }

    /// Returns a grid containing the records matching the given id
    /// `Ref`s.
    pub async fn read_by_ids(&mut self, ids: &[Ref]) -> Result<Grid> {
        let rows = ids.iter().map(|id| json!({"id": id.to_hayson()})).collect();

        let req_grid = Grid::new_internal(rows);
        self.post(self.read_url(), &req_grid).await
    }
}

impl HaystackUrl for SkySparkClient {
    fn about_url(&self) -> Url {
        self.append_to_url("about")
    }

    fn filetypes_url(&self) -> Url {
        self.append_to_url("filetypes")
    }

    fn his_read_url(&self) -> Url {
        self.append_to_url("hisRead")
    }

    fn his_write_url(&self) -> Url {
        self.append_to_url("hisWrite")
    }

    fn nav_url(&self) -> Url {
        self.append_to_url("nav")
    }

    fn ops_url(&self) -> Url {
        self.append_to_url("ops")
    }

    fn read_url(&self) -> Url {
        self.append_to_url("read")
    }
}

impl SkySparkClient {
    pub async fn eval(&mut self, axon_expr: &str) -> Result<Grid> {
        let row = json!({ "expr": axon_expr });
        let req_grid = Grid::new_internal(vec![row]);
        self.post(self.eval_url(), &req_grid).await
    }
}

async fn http_response_to_grid(res: reqwest::Response) -> Result<Grid> {
    let json: serde_json::Value = res.json().await?;
    let grid: Grid = json.try_into()?;

    if grid.is_error() {
        Err(Error::Grid { err_grid: grid })
    } else {
        Ok(grid)
    }
}

/// Returns true if the given URL appears to have the correct path
/// segments for a SkySpark API URL. The URL should end with a '/' character.
pub(crate) fn has_valid_path_segments(project_api_url: &Url) -> bool {
    if let Some(mut segments) = project_api_url.path_segments() {
        let api_literal = segments.next();
        let proj_name = segments.next();
        let blank = segments.next();
        let should_be_none = segments.next();

        match (api_literal, proj_name, blank, should_be_none) {
            (_, Some(""), _, _) => false,
            (Some("api"), Some(_), Some(""), None) => true,
            _ => false,
        }
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::api::HisReadRange;
    use crate::SkySparkClient;
    use crate::ValueExt;
    use raystack_core::{Number, Ref};
    use serde_json::json;
    use url::Url;

    fn project_api_url() -> Url {
        let url_str =
            std::env::var("RAYSTACK_SKYSPARK_PROJECT_API_URL").unwrap();
        Url::parse(&url_str).unwrap()
    }

    fn username() -> String {
        std::env::var("RAYSTACK_SKYSPARK_USERNAME").unwrap()
    }

    fn password() -> String {
        std::env::var("RAYSTACK_SKYSPARK_PASSWORD").unwrap()
    }

    async fn new_client() -> SkySparkClient {
        let username = username();
        let password = password();
        let reqwest_client = reqwest::Client::new();
        SkySparkClient::new_with_client(
            project_api_url(),
            &username,
            &password,
            reqwest_client,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn about() {
        let mut client = new_client().await;
        let grid = client.about().await.unwrap();
        assert_eq!(grid.rows()[0]["whoami"], json!(username()));
    }

    #[tokio::test]
    async fn filetypes() {
        let mut client = new_client().await;
        let grid = client.filetypes().await.unwrap();
        assert!(grid.rows()[0]["dis"].is_string());
    }

    #[tokio::test]
    async fn his_read_today() {
        let range = HisReadRange::Today;
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_yesterday() {
        let range = HisReadRange::Yesterday;
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date() {
        let range =
            HisReadRange::Date(chrono::NaiveDate::from_ymd(2019, 1, 1).into());
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date_span() {
        let range = HisReadRange::DateSpan {
            start: chrono::NaiveDate::from_ymd(2019, 1, 1).into(),
            end: chrono::NaiveDate::from_ymd(2019, 1, 2).into(),
        };
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date_time_span() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let start = DateTime::parse_from_rfc3339("2019-01-01T00:00:00+10:00")
            .unwrap()
            .with_timezone(&Sydney);
        let end = start + Duration::days(1);
        let range = HisReadRange::DateTimeSpan {
            start: start.into(),
            end: end.into(),
        };
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date_time() {
        use chrono::DateTime;
        use chrono_tz::Australia::Sydney;

        let date_time =
            DateTime::parse_from_rfc3339("2012-10-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let range = HisReadRange::SinceDateTime {
            date_time: date_time.into(),
        };
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date_time_utc() {
        use chrono::DateTime;
        use chrono_tz::Etc::UTC;

        let date_time = DateTime::parse_from_rfc3339("2012-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&UTC);
        let range = HisReadRange::SinceDateTime {
            date_time: date_time.into(),
        };
        his_read(&range).await;
    }

    async fn his_read(range: &HisReadRange) {
        let filter = format!("point and his and hisEnd");

        let mut client = new_client().await;
        let points_grid = client.read(&filter, Some(1)).await.unwrap();

        let point_ref = points_grid.rows()[0]["id"].as_hs_ref().unwrap();
        let his_grid = client.his_read(&point_ref, &range).await.unwrap();

        assert!(his_grid.meta()["hisStart"].is_hs_date_time());
        assert!(his_grid.meta()["hisEnd"].is_hs_date_time());
    }

    async fn get_ref_for_filter(
        client: &mut SkySparkClient,
        filter: &str,
    ) -> Ref {
        let points_grid = client.read(filter, Some(1)).await.unwrap();
        let point_ref = points_grid.rows()[0]["id"].as_hs_ref().unwrap();
        point_ref
    }

    #[tokio::test]
    async fn utc_his_write_bool() {
        use chrono::{DateTime, Duration, NaiveDateTime, Utc};

        let ndt = NaiveDateTime::parse_from_str(
            "2021-01-10 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        .unwrap();

        let date_time1 = DateTime::from_utc(ndt, Utc);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Bool\"",
        )
        .await;
        let his_data = vec![
            (date_time1, false),
            (date_time2, false),
            (date_time3, false),
        ];

        let res = client
            .utc_his_write_bool(&id, "Sydney", &his_data[..])
            .await
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn his_write_bool() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let mut client = new_client().await;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Bool\"",
        )
        .await;
        let his_data = vec![
            (date_time1.into(), true),
            (date_time2.into(), false),
            (date_time3.into(), true),
        ];

        let res = client.his_write_bool(&id, &his_data[..]).await.unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn utc_his_write_num() {
        use chrono::{Duration, NaiveDateTime, Utc};

        let ndt = NaiveDateTime::parse_from_str(
            "2021-01-10 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        .unwrap();

        let date_time1: chrono::DateTime<Utc> =
            chrono::DateTime::from_utc(ndt, Utc);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and unit",
        )
        .await;

        let unit = Some("L/s".to_owned());

        let his_data = vec![
            (date_time1, Number::new(111.111, unit.clone())),
            (date_time2, Number::new(222.222, unit.clone())),
            (date_time3, Number::new(333.333, unit.clone())),
        ];

        let res = client
            .utc_his_write_num(&id, "Sydney", &his_data[..])
            .await
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn his_write_num() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and unit",
        )
        .await;

        let unit = Some("L/s".to_owned());

        let his_data = vec![
            (date_time1.into(), Number::new(10.0, unit.clone())),
            (date_time2.into(), Number::new(15.34, unit.clone())),
            (date_time3.into(), Number::new(1.234, unit.clone())),
        ];

        let res = client.his_write_num(&id, &his_data[..]).await.unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn utc_his_write_num_no_unit() {
        use chrono::{Duration, NaiveDateTime, Utc};

        let ndt = NaiveDateTime::parse_from_str(
            "2021-01-10 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        .unwrap();

        let date_time1: chrono::DateTime<Utc> =
            chrono::DateTime::from_utc(ndt, Utc);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and not unit",
        )
        .await;
        let his_data = vec![
            (date_time1, Number::new_unitless(11.11)),
            (date_time2, Number::new_unitless(22.22)),
            (date_time3, Number::new_unitless(33.33)),
        ];

        let res = client
            .utc_his_write_num(&id, "Sydney", &his_data[..])
            .await
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn his_write_num_no_unit() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and not unit",
        )
        .await;

        let his_data = vec![
            (date_time1.into(), Number::new_unitless(10.0)),
            (date_time2.into(), Number::new_unitless(15.34)),
            (date_time3.into(), Number::new_unitless(1.234)),
        ];

        let res = client.his_write_num(&id, &his_data[..]).await.unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn utc_his_write_str() {
        use chrono::{DateTime, Duration, NaiveDateTime, Utc};

        let ndt = NaiveDateTime::parse_from_str(
            "2021-01-10 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        .unwrap();

        let date_time1 = DateTime::from_utc(ndt, Utc);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;
        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Str\"",
        )
        .await;

        let his_data = vec![
            (date_time1, "utc".to_owned()),
            (date_time2, "data".to_owned()),
            (date_time3, "here".to_owned()),
        ];

        let res = client
            .utc_his_write_str(&id, "Sydney", &his_data[..])
            .await
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn his_write_str() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client().await;
        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Str\"",
        )
        .await;

        let his_data = vec![
            (date_time1.into(), "hello".to_owned()),
            (date_time2.into(), "world".to_owned()),
            (date_time3.into(), "!".to_owned()),
        ];

        let res = client.his_write_str(&id, &his_data[..]).await.unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn nav_root() {
        let mut client = new_client().await;
        let grid = client.nav(None).await.unwrap();
        assert!(grid.rows()[0]["navId"].is_hs_ref());
    }

    #[tokio::test]
    async fn nav() {
        let mut client = new_client().await;
        let root_grid = client.nav(None).await.unwrap();
        let child_nav_id = root_grid.rows()[0]["navId"].as_hs_ref().unwrap();

        let child_grid = client.nav(Some(&child_nav_id)).await.unwrap();
        let final_nav_id = child_grid.rows()[0]["navId"].as_hs_ref().unwrap();
        assert_ne!(child_nav_id, final_nav_id);
    }

    #[tokio::test]
    async fn ops() {
        let mut client = new_client().await;
        let grid = client.ops().await.unwrap();
        assert_eq!(grid.rows()[0]["def"]["_kind"], "symbol");
    }

    #[tokio::test]
    async fn read_with_no_limit() {
        let mut client = new_client().await;
        let grid = client.read("point", None).await.unwrap();

        assert!(grid.rows()[0]["id"].is_hs_ref());
        assert!(grid.rows().len() > 10);
    }

    #[tokio::test]
    async fn read_with_zero_limit() {
        let mut client = new_client().await;
        let grid = client.read("id", Some(0)).await.unwrap();
        assert_eq!(grid.rows().len(), 0);
    }

    #[tokio::test]
    async fn read_with_non_zero_limit() {
        let mut client = new_client().await;
        let grid = client.read("id", Some(1)).await.unwrap();
        assert_eq!(grid.rows().len(), 1);

        let grid = client.read("id", Some(3)).await.unwrap();
        assert_eq!(grid.rows().len(), 3);
    }

    #[tokio::test]
    async fn read_by_ids_with_no_ids() {
        let mut client = new_client().await;
        let ids = vec![];
        let grid_result = client.read_by_ids(&ids).await;
        assert!(grid_result.is_err());
    }

    #[tokio::test]
    async fn read_by_ids_single() {
        let mut client = new_client().await;
        // Get some valid ids:
        let grid1 = client.read("id", Some(1)).await.unwrap();
        let ref1 = grid1.rows()[0]["id"].as_hs_ref().unwrap().clone();
        let ids = vec![ref1];
        let grid2 = client.read_by_ids(&ids).await.unwrap();
        assert_eq!(grid1, grid2);
    }

    #[tokio::test]
    async fn read_by_ids_multiple() {
        let mut client = new_client().await;
        // Get some valid ids:
        let grid1 = client.read("id", Some(2)).await.unwrap();
        let ref1 = grid1.rows()[0]["id"].as_hs_ref().unwrap().clone();
        let ref2 = grid1.rows()[1]["id"].as_hs_ref().unwrap().clone();

        let ids = vec![ref1, ref2];
        let grid2 = client.read_by_ids(&ids).await.unwrap();
        assert_eq!(grid1, grid2);
    }

    #[tokio::test]
    async fn eval() {
        let mut client = new_client().await;
        let axon_expr = "readAll(id and mod)[0..1].keepCols([\"id\", \"mod\"])";
        let grid = client.eval(axon_expr).await.unwrap();
        assert!(grid.rows()[0]["id"].is_hs_ref());
    }

    #[test]
    fn add_backslash_necessary() {
        use crate::add_backslash_if_necessary;
        let url = Url::parse("http://www.example.com/api/proj").unwrap();
        let expected = Url::parse("http://www.example.com/api/proj/").unwrap();
        assert_eq!(add_backslash_if_necessary(url), expected);
    }

    #[test]
    fn add_backslash_not_necessary() {
        use crate::add_backslash_if_necessary;
        let url = Url::parse("http://www.example.com/api/proj/").unwrap();
        let expected = Url::parse("http://www.example.com/api/proj/").unwrap();
        assert_eq!(add_backslash_if_necessary(url), expected);
    }

    #[tokio::test]
    async fn error_grid() {
        use crate::err::Error;

        let mut client = new_client().await;
        let grid_result = client.eval("reabDDDAll(test").await;

        assert!(grid_result.is_err());
        let err = grid_result.err().unwrap();

        match err {
            Error::Grid { err_grid } => {
                assert!(err_grid.is_error());
                assert!(err_grid.error_trace().is_some());
            }
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn project_name_works() {
        let client = new_client().await;
        assert!(client.project_name().len() > 3);
    }

    #[test]
    fn has_valid_path_segments_works() {
        use super::has_valid_path_segments;

        let good_url = Url::parse("http://www.test.com/api/proj/").unwrap();
        assert!(has_valid_path_segments(&good_url));
        let bad_url1 = Url::parse("http://www.test.com/api/proj").unwrap();
        assert!(!has_valid_path_segments(&bad_url1));
        let bad_url2 = Url::parse("http://www.test.com/api/").unwrap();
        assert!(!has_valid_path_segments(&bad_url2));
        let bad_url3 =
            Url::parse("http://www.test.com/api/proj/extra").unwrap();
        assert!(!has_valid_path_segments(&bad_url3));
        let bad_url4 = Url::parse("http://www.test.com").unwrap();
        assert!(!has_valid_path_segments(&bad_url4));
        let bad_url5 = Url::parse("http://www.test.com/api//extra").unwrap();
        assert!(!has_valid_path_segments(&bad_url5));
    }

    #[tokio::test]
    async fn recovers_from_invalid_auth_token() {
        let mut client = new_client().await;

        let bad_token = "badauthtoken";

        assert_ne!(client.test_auth_token(), bad_token);

        // Check the client works before modifying the auth token:
        let grid1 = client.about().await.unwrap();
        assert_eq!(grid1.rows()[0]["whoami"], json!(username()));

        client.test_manually_set_auth_token(bad_token);
        assert_eq!(client.test_auth_token(), bad_token);

        // Check the client still works after setting a bad auth token:
        let grid2 = client.about().await.unwrap();
        assert_eq!(grid2.rows()[0]["whoami"], json!(username()));
    }
}
