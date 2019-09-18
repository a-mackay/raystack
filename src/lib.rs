//! # Overview
//! This crate provides functions which can query a SkySpark server, using
//! the Haystack REST API and the SkySpark REST API's `eval` operation.
//! Some Haystack operations are not implemented
//! (watch* operations, pointWrite and invokeAction).
//!
//! # Usage
//! 1. Create a new rust project: ```cargo new --bin my_project```
//! 1. Put these dependencies in your Cargo.toml:
//!     ```toml
//!     [dependencies]
//!     chrono = "0.4.7"
//!     chrono-tz = "0.5.1"
//!     raystack = "0.5"
//!     serde_json = "1.0.40"
//!     url = "1.7.2"
//!     ```
//! 1. Put this in `main.rs` to create and use a `SkySparkClient`:
//!     ```rust,no_run
//!     use raystack::{
//!         HaystackRest,
//!         SkySparkClient,
//!         SkySparkRest,
//!         ValueExt,
//!     };
//!     use url::Url;
//!     
//!     fn main() {
//!         let url = Url::parse("https://www.example.com/api/projName/").unwrap();
//!         let client = SkySparkClient::new(url, "username", "p4ssw0rd", None).unwrap();
//!         let sites_grid = client.eval("readAll(site)").unwrap();
//!     
//!         // Print the raw JSON:
//!         println!("{}", sites_grid.to_string_pretty());
//!     
//!         // Working with the Grid struct:
//!         println!("All columns: {:?}", sites_grid.cols());
//!         println!("first site id: {:?}", sites_grid.rows()[0]["id"].as_hs_ref().unwrap());
//!     }
//!     ```
//!
//! The Grid struct is a wrapper around the underlying JSON Value enum
//! provided by the `serde_json` crate. See the
//! [documentation for Value](https://docs.serde.rs/serde_json/enum.Value.html)
//! for more information on how to query for data stored within it.

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
pub use api::{HisReadRange};
use chrono::DateTime;
use chrono_tz::Tz;
pub use coord::Coord;
pub use err::{Error, ErrorKind};
pub use grid::{Grid, ParseJsonGridError};
pub use hsref::{ParseRefError, Ref};
pub use number::{Number, ParseNumberError};
use reqwest::Client as ReqwestClient;
use serde_json::json;
use std::convert::TryInto;
pub use tag::{is_tag_name, ParseTagNameError, TagName};
use url::Url;
pub use value_ext::ValueExt;

type Result<T> = std::result::Result<T, Error>;

/// A client for interacting with a SkySpark server.
#[derive(Debug)]
pub struct SkySparkClient {
    auth_token: String,
    client: ReqwestClient,
    project_api_url: Url,
}

impl SkySparkClient {
    /// Create a new `SkySparkClient`.
    /// # Example
    /// ```rust,no_run
    /// use raystack::SkySparkClient;
    /// use url::Url;
    /// let url = Url::parse("https://skyspark.company.com/api/bigProject/").unwrap();
    /// let client = SkySparkClient::new(url, "username", "p4ssw0rd", None).unwrap();
    /// ```
    pub async fn new(
        project_api_url: Url,
        username: &str,
        password: &str,
        timeout_in_seconds: Option<u64>,
    ) -> Result<Self> {
        use std::time::Duration;

        let project_api_url = add_backslash_if_necessary(project_api_url);

        let client = if let Some(timeout) = timeout_in_seconds {
            ReqwestClient::builder()
            .timeout(Duration::from_secs(timeout))
            .build()?
        } else {
            ReqwestClient::new()
        };

        let mut auth_url = project_api_url.clone();
        auth_url.set_path("/ui");

        let auth_token = auth::new_auth_token(
            &client,
            auth_url.as_str(),
            username,
            password,
        ).await?;

        Ok(SkySparkClient {
            auth_token,
            client,
            project_api_url,
        })
    }

    fn auth_header_value(&self) -> String {
        format!("BEARER authToken={}", self.auth_token)
    }

    fn eval_url(&self) -> Url {
        self.append_to_url("eval")
    }

    async fn get(&self, url: Url) -> Result<Grid> {
        let res: Result<reqwest::Response> = self.client
            .get(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .send()
            .await
            .map_err(|err| err.into());

        Self::res_to_grid(res?).await
    }

    async fn post(&self, url: Url, grid: &Grid) -> Result<Grid> {
        let res: Result<reqwest::Response> = self.client
            .post(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .header("Content-Type", "application/json")
            .body(grid.to_string())
            .send()
            .await
            .map_err(|err| err.into());

        Self::res_to_grid(res?).await
    }

    async fn res_to_grid(mut res: reqwest::Response) -> Result<Grid> {
        let json: serde_json::Value = res.json().await?;
        let grid: Grid = json.try_into()?;

        if grid.is_error() {
            Err(Error::new(ErrorKind::Grid { err_grid: grid }))
        } else {
            Ok(grid)
        }
    }

    fn append_to_url(&self, s: &str) -> Url {
        self.project_api_url
            .join(s)
            .expect("since url ends with '/' this should never fail")
    }

    /// Return the project name for this client. If the url given to the
    /// `SkySparkClient` was correct, then this function should return a
    /// project name.
    pub fn project_name(&self) -> Option<&str> {
        let mut path_split =
            self.project_api_url.path_segments()?.collect::<Vec<_>>();
        match path_split.pop() {
            Some("") => path_split.pop(), // If empty, get the second last element
            last_elem @ Some(_) => last_elem,
            None => None,
        }
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

    pub async fn his_read(&self, id: &Ref, range: &HisReadRange) -> Result<Grid> {
        let row = json!({
            "id": id.to_encoded_json_string(),
            "range": range.to_string()
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

    async fn ops(&self) -> Result<Grid> {
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

#[cfg(test)]
mod test {
    use crate::api::{HisReadRange};
    use crate::grid::Grid;
    use crate::hsref::Ref;
    use crate::SkySparkClient;
    use serde_json::json;
    use url::Url;

    fn project_api_url() -> Url {
        let url_str = std::env::var("RUST_SKYSPARK_PROJECT_API_URL").unwrap();
        Url::parse(&url_str).unwrap()
    }

    fn username() -> String {
        std::env::var("RUST_SKYSPARK_USERNAME").unwrap()
    }

    fn password() -> String {
        std::env::var("RUST_SKYSPARK_PASSWORD").unwrap()
    }

    async fn new_client() -> SkySparkClient {
        let username = username();
        let password = password();
        SkySparkClient::new(project_api_url(), &username, &password, None)
            .await
            .unwrap()
    }

    fn pprint(grid: &Grid) {
        println!("\n{}", grid.to_string_pretty());
    }

    #[tokio::test]
    async fn about() {
        let client = new_client().await;
        let grid = client.about().await.unwrap();
        pprint(&grid);
        assert_eq!(grid.rows()[0]["whoami"], json!(username()));
    }

    #[tokio::test]
    async fn formats() {
        let client = new_client().await;
        let grid = client.formats().await.unwrap();
        pprint(&grid);
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
        pprint(&points_grid);

        let point_ref_str = points_grid.rows()[0]["id"].as_str().unwrap();
        let point_ref = Ref::from_encoded_json_string(&point_ref_str).unwrap();
        let his_grid = client.his_read(&point_ref, &range).await.unwrap();
        pprint(&his_grid);

        assert!(his_grid.meta()["hisStart"].is_string());
        assert!(his_grid.meta()["hisEnd"].is_string());
    }

    #[tokio::test]
    async fn his_write_bool() {
        assert_eq!("Add a matching point to the project", "");
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let client = new_client().await;
        let id =
            Ref::new("@p:the_project:r:24efe1c4-24aef280".to_owned()).unwrap();
        let his_data =
            vec![(date_time1, true), (date_time2, false), (date_time3, true)];

        let res = client.his_write_bool(&id, &his_data[..]).await.unwrap();
        pprint(&res);
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn his_write_num() {
        assert_eq!("Add a matching point to the project", "");
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let client = new_client().await;
        let id =
            Ref::new("@p:the_project:r:24efe317-acdc8f48".to_owned()).unwrap();
        let his_data =
            vec![(date_time1, 10.0), (date_time2, 15.34), (date_time3, 1.234)];

        let res = client.his_write_num(&id, &his_data[..], "L/s").await.unwrap();
        pprint(&res);
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn his_write_str() {
        assert_eq!("Add a matching point to the project", "");
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let client = new_client().await;
        let id =
            Ref::new("@p:the_project:r:24efdc96-96baaf9d".to_owned()).unwrap();
        let his_data = vec![
            (date_time1, "hello".to_owned()),
            (date_time2, "world".to_owned()),
            (date_time3, "!".to_owned()),
        ];

        let res = client.his_write_str(&id, &his_data[..]).await.unwrap();
        pprint(&res);
        assert_eq!(res.rows().len(), 0);
    }

    #[tokio::test]
    async fn nav_root() {
        let client = new_client().await;
        let grid = client.nav(None).await.unwrap();
        pprint(&grid);
        assert!(grid.rows()[0]["navId"].is_string());
    }

    #[tokio::test]
    async fn nav() {
        let client = new_client().await;
        let root_grid = client.nav(None).await.unwrap();
        let child_nav_id = root_grid.rows()[0]["navId"].as_str().unwrap();

        let child_grid = client.nav(Some(&child_nav_id)).await.unwrap();
        pprint(&child_grid);
        let final_nav_id = child_grid.rows()[0]["navId"].as_str().unwrap();
        assert_ne!(child_nav_id, final_nav_id);
    }

    #[tokio::test]
    async fn ops() {
        let client = new_client().await;
        let grid = client.ops().await.unwrap();
        pprint(&grid);
        assert!(grid.rows()[0]["name"].is_string());
    }

    #[tokio::test]
    async fn read_with_no_limit() {
        let client = new_client().await;
        let grid = client.read("projMeta or uiMeta", None).await.unwrap();
        pprint(&grid);

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
        pprint(&grid);
        assert_eq!(grid.rows().len(), 0);
    }

    #[tokio::test]
    async fn read_with_non_zero_limit() {
        let client = new_client().await;
        let grid = client.read("id", Some(1)).await.unwrap();
        pprint(&grid);
        assert_eq!(grid.rows().len(), 1);

        let grid = client.read("id", Some(3)).await.unwrap();
        pprint(&grid);
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
        pprint(&grid2);
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
        pprint(&grid2);
        assert_eq!(grid1, grid2);
    }

    #[tokio::test]
    async fn eval() {
        let client = new_client().await;
        let axon_expr = "readAll(id and mod)[0..1].keepCols([\"id\", \"mod\"])";
        let grid = client.eval(axon_expr).await.unwrap();
        pprint(&grid);
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
        use crate::err::ErrorKind;

        let client = new_client().await;
        let grid_result = client.eval("reabDDDAll(test").await;

        assert!(grid_result.is_err());
        let err = grid_result.err().unwrap();
        let kind = err.kind();

        match kind {
            ErrorKind::Grid { err_grid } => {
                assert!(err_grid.is_error());
                assert!(err_grid.error_trace().is_some());
            }
            _ => panic!(),
        }
    }
}
