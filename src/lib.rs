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
//! use raystack::{ClientSeed, SkySparkClient, ValueExt};
//! use url::Url;
//!
//! let timeout_in_seconds = 30;
//! let client_seed = ClientSeed::new(timeout_in_seconds).unwrap();
//! let url = Url::parse("https://www.example.com/api/projName/").unwrap();
//! let client = SkySparkClient::new(url, "username", "p4ssw0rd", client_seed).await.unwrap();
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
mod coord;
mod err;
mod grid;
mod hsref;
mod number;
mod tag;
mod value_ext;

use api::HaystackUrl;
pub use api::HisReadRange;
use chrono::DateTime;
use chrono_tz::Tz;
pub use coord::Coord;
pub use err::{Error, NewClientSeedError, NewSkySparkClientError};
pub use grid::{Grid, ParseJsonGridError};
pub use hsref::{ParseRefError, Ref};
pub use number::{Number, ParseNumberError};
use reqwest::Client as ReqwestClient;
use serde_json::json;
use std::convert::TryInto;
pub use tag::{is_tag_name, ParseTagNameError, TagName};
use thiserror::Error;
use url::Url;
pub use value_ext::ValueExt;

type Result<T> = std::result::Result<T, Error>;
type StdResult<T, E> = std::result::Result<T, E>;

/// Contains resources used by a `SkySparkClient`. If creating multiple
/// `SkySparkClient`s, the same `ClientSeed` should be reused for each,
/// by calling the `.clone()` method.
#[derive(Clone, Debug)]
pub struct ClientSeed {
    client: ReqwestClient,
    rng: ring::rand::SystemRandom,
}

impl ClientSeed {
    /// Create a new `ClientSeed`. The timeout determines how long the
    /// underlying HTTP library will wait before timing out.
    pub fn new(timeout_in_seconds: u64) -> StdResult<Self, NewClientSeedError> {
        use std::time::Duration;

        let client = ReqwestClient::builder()
            .timeout(Duration::from_secs(timeout_in_seconds))
            .build()?;

        Ok(Self {
            client,
            rng: ring::rand::SystemRandom::new(),
        })
    }

    fn client(&self) -> &reqwest::Client {
        &self.client
    }

    fn rng(&self) -> &ring::rand::SystemRandom {
        &self.rng
    }
}

/// A client for interacting with a SkySpark server.
#[derive(Debug)]
pub struct SkySparkClient {
    auth_token: String,
    client_seed: ClientSeed,
    project_api_url: Url,
}

impl SkySparkClient {
    /// Create a new `SkySparkClient`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn run() {
    /// use raystack::{ClientSeed, SkySparkClient};
    /// use url::Url;
    /// let timeout_in_seconds = 30;
    /// let client_seed = ClientSeed::new(timeout_in_seconds).unwrap();
    /// let url = Url::parse("https://skyspark.company.com/api/bigProject/").unwrap();
    /// let client = SkySparkClient::new(url, "username", "p4ssw0rd", client_seed).await.unwrap();
    /// # }
    /// ```
    ///
    /// If creating multiple `SkySparkClient`s,
    /// the same `ClientSeed` should be used for each. For example:
    ///
    /// ```rust,no_run
    /// # async fn run() {
    /// use raystack::{ClientSeed, SkySparkClient};
    /// use url::Url;
    /// let client_seed = ClientSeed::new(30).unwrap();
    /// let url1 = Url::parse("http://test.com/api/bigProject/").unwrap();
    /// let client1 = SkySparkClient::new(url1, "name", "p4ssw0rd", client_seed.clone()).await.unwrap();
    /// let url2 = Url::parse("http://test.com/api/smallProj/").unwrap();
    /// let client2 = SkySparkClient::new(url2, "name", "p4ss", client_seed.clone()).await.unwrap();
    /// # }
    /// ```
    ///
    /// We pass in the `ClientSeed`
    /// struct because the underlying crypto library recommends that an
    /// application should create a single random number generator and use
    /// it for all randomness generation. Additionally, the underlying HTTP
    /// library recommends using a single copy of its HTTP client. These two
    /// resources are wrapped by this `ClientSeed` struct.
    pub async fn new(
        project_api_url: Url,
        username: &str,
        password: &str,
        client_seed: ClientSeed,
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

        let mut auth_url = project_api_url.clone();
        auth_url.set_path("/ui");

        let auth_token = auth::new_auth_token(
            client_seed.client(),
            auth_url.as_str(),
            username,
            password,
            client_seed.rng(),
        )
        .await
        .map_err(crate::auth::AuthError::from)?;

        Ok(SkySparkClient {
            auth_token,
            client_seed,
            project_api_url,
        })
    }

    fn client(&self) -> &reqwest::Client {
        self.client_seed.client()
    }

    fn auth_header_value(&self) -> String {
        format!("BEARER authToken={}", self.auth_token)
    }

    fn eval_url(&self) -> Url {
        self.append_to_url("eval")
    }

    async fn get(&self, url: Url) -> Result<Grid> {
        let res: Result<reqwest::Response> = self
            .client()
            .get(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .send()
            .await
            .map_err(|err| err.into());

        Self::res_to_grid(res?).await
    }

    async fn post(&self, url: Url, grid: &Grid) -> Result<Grid> {
        let res: Result<reqwest::Response> = self
            .client()
            .post(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .header("Content-Type", "application/json")
            .body(grid.to_json_string())
            .send()
            .await
            .map_err(|err| err.into());

        Self::res_to_grid(res?).await
    }

    async fn res_to_grid(res: reqwest::Response) -> Result<Grid> {
        let json: serde_json::Value = res.json().await?;
        let grid: Grid = json.try_into()?;

        if grid.is_error() {
            Err(Error::Grid { err_grid: grid })
        } else {
            Ok(grid)
        }
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
            .skip(1)
            .next()
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
fn add_backslash_if_necessary(url: Url) -> Url {
    let chars = url.as_str().chars().collect::<Vec<_>>();
    let last_char = chars.last().expect("parsed url should have >= 1 chars");
    if *last_char != '/' {
        Url::parse(&(url.to_string() + "/")).expect("adding '/' to the end of a parsable url should create another parsable url")
    } else {
        url
    }
}

impl SkySparkClient {
    pub async fn about(&self) -> Result<Grid> {
        self.get(self.about_url()).await
    }

    pub async fn formats(&self) -> Result<Grid> {
        self.get(self.formats_url()).await
    }

    pub async fn his_read(
        &self,
        id: &Ref,
        range: &HisReadRange,
    ) -> Result<Grid> {
        let row = json!({
            "id": id.to_encoded_json_string(),
            "range": range.to_json_request_string()
        });
        let req_grid = Grid::new_internal(vec![row]);

        self.post(self.his_read_url(), &req_grid).await
    }

    pub async fn his_write_bool(
        &self,
        id: &Ref,
        his_data: &[(DateTime<Tz>, bool)],
    ) -> Result<Grid> {
        use api::to_zinc_encoded_string;

        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                json!({
                    "ts": format!("t:{}", to_zinc_encoded_string(date_time)),
                    "val": value
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    pub async fn his_write_num(
        &self,
        id: &Ref,
        his_data: &[(DateTime<Tz>, f64)],
        unit: &str,
    ) -> Result<Grid> {
        use api::to_zinc_encoded_string;

        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                json!({
                    "ts": format!("t:{}", to_zinc_encoded_string(date_time)),
                    "val": format!("n:{} {}", value, unit)
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    pub async fn his_write_str(
        &self,
        id: &Ref,
        his_data: &[(DateTime<Tz>, String)],
    ) -> Result<Grid> {
        use api::to_zinc_encoded_string;

        let rows = his_data
            .iter()
            .map(|(date_time, value)| {
                json!({
                    "ts": format!("t:{}", to_zinc_encoded_string(date_time)),
                    "val": value
                })
            })
            .collect();

        let mut req_grid = Grid::new_internal(rows);
        req_grid.add_ref_to_meta(id);

        self.post(self.his_write_url(), &req_grid).await
    }

    pub async fn nav(&self, nav_id: Option<&str>) -> Result<Grid> {
        let req_grid = match nav_id {
            Some(nav_id) => {
                let row = json!({ "navId": nav_id });
                Grid::new_internal(vec![row])
            }
            None => Grid::new_internal(Vec::new()),
        };

        self.post(self.nav_url(), &req_grid).await
    }

    pub async fn ops(&self) -> Result<Grid> {
        self.get(self.ops_url()).await
    }

    pub async fn read(&self, filter: &str, limit: Option<u64>) -> Result<Grid> {
        let row = match limit {
            Some(integer) => json!({"filter": filter, "limit": integer}),
            None => json!({"filter": filter, "limit": "N"}),
        };

        let req_grid = Grid::new_internal(vec![row]);
        self.post(self.read_url(), &req_grid).await
    }

    pub async fn read_by_ids(&self, ids: &[Ref]) -> Result<Grid> {
        let rows = ids
            .iter()
            .map(|id| json!({"id": id.to_encoded_json_string()}))
            .collect();

        let req_grid = Grid::new_internal(rows);
        self.post(self.read_url(), &req_grid).await
    }
}

impl HaystackUrl for SkySparkClient {
    fn about_url(&self) -> Url {
        self.append_to_url("about")
    }

    fn formats_url(&self) -> Url {
        self.append_to_url("formats")
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
    pub async fn eval(&self, axon_expr: &str) -> Result<Grid> {
        let row = json!({ "expr": axon_expr });
        let req_grid = Grid::new_internal(vec![row]);
        self.post(self.eval_url(), &req_grid).await
    }
}

/// Returns true if the given URL appears to have the correct path
/// segments for a SkySpark API URL. The URL should end with a '/' character.
fn has_valid_path_segments(project_api_url: &Url) -> bool {
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
    use crate::hsref::Ref;
    use crate::ClientSeed;
    use crate::SkySparkClient;
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
        let seed = ClientSeed::new(15).unwrap();
        SkySparkClient::new(project_api_url(), &username, &password, seed)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn about() {
        let client = new_client().await;
        let grid = client.about().await.unwrap();
        assert_eq!(grid.rows()[0]["whoami"], json!(username()));
    }

    #[tokio::test]
    async fn formats() {
        let client = new_client().await;
        let grid = client.formats().await.unwrap();
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
        let range = HisReadRange::Date(chrono::NaiveDate::from_ymd(2019, 1, 1));
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date_span() {
        let range = HisReadRange::DateSpan {
            start: chrono::NaiveDate::from_ymd(2019, 1, 1),
            end: chrono::NaiveDate::from_ymd(2019, 1, 2),
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
        let range = HisReadRange::DateTimeSpan { start, end };
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
        let range = HisReadRange::SinceDateTime { date_time };
        his_read(&range).await;
    }

    #[tokio::test]
    async fn his_read_date_time_utc() {
        use chrono::DateTime;
        use chrono_tz::Etc::UTC;

        let date_time = DateTime::parse_from_rfc3339("2012-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&UTC);
        let range = HisReadRange::SinceDateTime { date_time };
        his_read(&range).await;
    }

    async fn his_read(range: &HisReadRange) {
        let filter = format!("point and his and hisEnd");

        let client = new_client().await;
        let points_grid = client.read(&filter, Some(1)).await.unwrap();

        let point_ref_str = points_grid.rows()[0]["id"].as_str().unwrap();
        let point_ref = Ref::from_encoded_json_string(&point_ref_str).unwrap();
        let his_grid = client.his_read(&point_ref, &range).await.unwrap();

        assert!(his_grid.meta()["hisStart"].is_string());
        assert!(his_grid.meta()["hisEnd"].is_string());
    }

    async fn get_ref_for_filter(client: &SkySparkClient, filter: &str) -> Ref {
        let points_grid = client.read(filter, Some(1)).await.unwrap();
        let point_ref = points_grid.rows()[0]["id"]
            .as_str()
            .and_then(|ref_str| Ref::from_encoded_json_string(ref_str).ok())
            .unwrap();
        point_ref
    }

    #[tokio::test]
    async fn his_write_bool() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let client = new_client().await;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let id = get_ref_for_filter(
            &client,
            "continuousIntegrationHisWritePoint and kind == \"Bool\"",
        )
        .await;
        let his_data =
            vec![(date_time1, true), (date_time2, false), (date_time3, true)];

        let res = client.his_write_bool(&id, &his_data[..]).await.unwrap();
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

        let client = new_client().await;

        let id = get_ref_for_filter(
            &client,
            "continuousIntegrationHisWritePoint and kind == \"Number\"",
        )
        .await;
        let his_data =
            vec![(date_time1, 10.0), (date_time2, 15.34), (date_time3, 1.234)];

        let res = client
            .his_write_num(&id, &his_data[..], "L/s")
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

        let client = new_client().await;
        let id = get_ref_for_filter(
            &client,
            "continuousIntegrationHisWritePoint and kind == \"Str\"",
        )
        .await;

        let his_data = vec![
            (date_time1, "hello".to_owned()),
            (date_time2, "world".to_owned()),
            (date_time3, "!".to_owned()),
        ];

        let res = client.his_write_str(&id, &his_data[..]).await.unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn nav_root() {
        let client = new_client().await;
        let grid = client.nav(None).await.unwrap();
        assert!(grid.rows()[0]["navId"].is_string());
    }

    #[tokio::test]
    async fn nav() {
        let client = new_client().await;
        let root_grid = client.nav(None).await.unwrap();
        let child_nav_id = root_grid.rows()[0]["navId"].as_str().unwrap();

        let child_grid = client.nav(Some(&child_nav_id)).await.unwrap();
        let final_nav_id = child_grid.rows()[0]["navId"].as_str().unwrap();
        assert_ne!(child_nav_id, final_nav_id);
    }

    #[tokio::test]
    async fn ops() {
        let client = new_client().await;
        let grid = client.ops().await.unwrap();
        assert!(grid.rows()[0]["name"].is_string());
    }

    #[tokio::test]
    async fn read_with_no_limit() {
        let client = new_client().await;
        let grid = client.read("projMeta or uiMeta", None).await.unwrap();

        assert!(grid.rows()[0]["id"].is_string());
        let proj_meta = &grid.rows()[0]["projMeta"];
        let ui_meta = &grid.rows()[0]["uiMeta"];
        let marker = json!("m:");
        assert!(*proj_meta == marker || *ui_meta == marker);
    }

    #[tokio::test]
    async fn read_with_zero_limit() {
        let client = new_client().await;
        let grid = client.read("id", Some(0)).await.unwrap();
        assert_eq!(grid.rows().len(), 0);
    }

    #[tokio::test]
    async fn read_with_non_zero_limit() {
        let client = new_client().await;
        let grid = client.read("id", Some(1)).await.unwrap();
        assert_eq!(grid.rows().len(), 1);

        let grid = client.read("id", Some(3)).await.unwrap();
        assert_eq!(grid.rows().len(), 3);
    }

    #[tokio::test]
    async fn read_by_ids_with_no_ids() {
        let client = new_client().await;
        let ids = vec![];
        let grid_result = client.read_by_ids(&ids).await;
        assert!(grid_result.is_err());
    }

    #[tokio::test]
    async fn read_by_ids_single() {
        let client = new_client().await;
        // Get some valid ids:
        let grid1 = client.read("id", Some(1)).await.unwrap();
        let raw_id1 = &grid1.rows()[0]["id"].as_str().unwrap();
        let ref1 = Ref::from_encoded_json_string(raw_id1).unwrap();
        let ids = vec![ref1];
        let grid2 = client.read_by_ids(&ids).await.unwrap();
        assert_eq!(grid1, grid2);
    }

    #[tokio::test]
    async fn read_by_ids_multiple() {
        let client = new_client().await;
        // Get some valid ids:
        let grid1 = client.read("id", Some(2)).await.unwrap();
        let raw_id1 = &grid1.rows()[0]["id"].as_str().unwrap();
        let raw_id2 = &grid1.rows()[1]["id"].as_str().unwrap();
        let ref1 = Ref::from_encoded_json_string(raw_id1).unwrap();
        let ref2 = Ref::from_encoded_json_string(raw_id2).unwrap();

        let ids = vec![ref1, ref2];
        let grid2 = client.read_by_ids(&ids).await.unwrap();
        assert_eq!(grid1, grid2);
    }

    #[tokio::test]
    async fn eval() {
        let client = new_client().await;
        let axon_expr = "readAll(id and mod)[0..1].keepCols([\"id\", \"mod\"])";
        let grid = client.eval(axon_expr).await.unwrap();
        assert!(grid.rows()[0]["id"].is_string());
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

        let client = new_client().await;
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
        assert!(client.project_name().ends_with("dev"));
    }

    #[test]
    fn has_valid_path_segments() {
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
}
