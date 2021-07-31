use crate::api::HaystackUrl;
use crate::api::HisReadRange;
use crate::err::{Error, NewSkySparkClientError};
use crate::grid::Grid;
use crate::hs_types::DateTime;
use crate::tz::skyspark_tz_string_to_tz;
use chrono::Utc;
use raystack_core::Hayson;
use raystack_core::Number;
use raystack_core::Ref;
use serde_json::json;
use std::convert::TryInto;
use url::Url;

type Result<T> = std::result::Result<T, Error>;
type StdResult<T, E> = std::result::Result<T, E>;

pub(crate) fn new_auth_token(
    project_api_url: &Url,
    // reqwest_client: &reqwest::blocking::Client,
    username: &str,
    password: &str,
) -> StdResult<String, crate::auth::AuthError> {
    let mut auth_url = project_api_url.clone();
    auth_url.set_path("/ui");

    // TODO Falling back to using the async Client to keep things simple,
    // ideally this should using the blocking Client to be consistent.
    let reqwest_client = reqwest::Client::new();

    let auth_token = futures::executor::block_on(crate::auth::new_auth_token(
        &reqwest_client,
        auth_url.as_str(),
        username,
        password,
    ))?;

    Ok(auth_token)
}

/// A client for interacting with a SkySpark server.
#[derive(Debug)]
pub struct SkySparkClient {
    auth_token: String,
    client: reqwest::blocking::Client,
    username: String,
    password: String,
    project_api_url: Url,
}

impl SkySparkClient {
    /// Create a new `SkySparkClient`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn run() {
    /// use raystack::SkySparkClient;
    /// use url::Url;
    /// let url = Url::parse("https://skyspark.company.com/api/bigProject/").unwrap();
    /// let mut client = SkySparkClient::new(url, "username", "p4ssw0rd").unwrap();
    /// # }
    /// ```
    pub fn new(
        project_api_url: Url,
        username: &str,
        password: &str,
    ) -> std::result::Result<Self, NewSkySparkClientError> {
        let client = reqwest::blocking::Client::new();
        Self::new_with_client(project_api_url, username, password, client)
    }

    /// Create a new `SkySparkClient`, passing in an existing
    /// `reqwest::Client`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn run() {
    /// use raystack::SkySparkClient;
    /// use reqwest::blocking::Client;
    /// use url::Url;
    /// let reqwest_client = Client::new();
    /// let url = Url::parse("https://skyspark.company.com/api/bigProject/").unwrap();
    /// let mut client = SkySparkClient::new_with_client(url, "username", "p4ssw0rd", reqwest_client).unwrap();
    /// # }
    /// ```
    ///
    /// If creating multiple `SkySparkClient`s,
    /// the same `reqwest::Client` should be used for each. For example:
    ///
    /// ```rust,no_run
    /// # fn run() {
    /// use raystack::SkySparkClient;
    /// use reqwest::blocking::Client;
    /// use url::Url;
    /// let reqwest_client = Client::new();
    /// let url1 = Url::parse("http://test.com/api/bigProject/").unwrap();
    /// let client1 = SkySparkClient::new_with_client(url1, "name", "password", reqwest_client.clone()).unwrap();
    /// let url2 = Url::parse("http://test.com/api/smallProj/").unwrap();
    /// let client2 = SkySparkClient::new_with_client(url2, "name", "password", reqwest_client.clone()).unwrap();
    /// # }
    /// ```
    pub fn new_with_client(
        project_api_url: Url,
        username: &str,
        password: &str,
        reqwest_client: reqwest::blocking::Client,
    ) -> std::result::Result<Self, NewSkySparkClientError> {
        let project_api_url = crate::add_backslash_if_necessary(project_api_url);

        if project_api_url.cannot_be_a_base() {
            let url_err_msg = "the project API URL must be a valid base URL";
            return Err(NewSkySparkClientError::url(url_err_msg));
        }

        if !crate::has_valid_path_segments(&project_api_url) {
            let url_err_msg = "URL must be formatted similarly to http://www.test.com/api/project/";
            return Err(NewSkySparkClientError::url(url_err_msg));
        }

        Ok(SkySparkClient {
            auth_token: new_auth_token(
                &project_api_url,
                username,
                password,
            )?,
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

    fn update_auth_token(&mut self) -> StdResult<(), crate::auth::AuthError> {
        let auth_token = new_auth_token(
            self.project_api_url(),
            &self.username,
            &self.password,
        )?;
        self.auth_token = auth_token;
        Ok(())
    }

    pub fn client(&self) -> &reqwest::blocking::Client {
        &self.client
    }

    fn auth_header_value(&self) -> String {
        format!("BEARER authToken={}", self.auth_token)
    }

    fn eval_url(&self) -> Url {
        self.append_to_url("eval")
    }

    fn get(&mut self, url: Url) -> Result<Grid> {
        let res = self.get_response(url.clone())?;

        if res.status() == reqwest::StatusCode::FORBIDDEN {
            self.update_auth_token()?;
            let retry_res = self.get_response(url)?;
            http_response_to_grid(retry_res)
        } else {
            http_response_to_grid(res)
        }
    }

    fn get_response(&self, url: Url) -> Result<reqwest::blocking::Response> {
        self.client()
            .get(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .send()
            .map_err(|err| err.into())
    }

    fn post(&mut self, url: Url, grid: &Grid) -> Result<Grid> {
        let res = self.post_response(url.clone(), grid)?;

        if res.status() == reqwest::StatusCode::FORBIDDEN {
            self.update_auth_token()?;
            let retry_res = self.post_response(url, grid)?;
            http_response_to_grid(retry_res)
        } else {
            http_response_to_grid(res)
        }
    }

    fn post_response(
        &self,
        url: Url,
        grid: &Grid,
    ) -> Result<reqwest::blocking::Response> {
        self.client()
            .post(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth_header_value())
            .header("Content-Type", "application/json")
            .body(grid.to_json_string())
            .send()
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

impl SkySparkClient {
    /// Returns a grid containing basic server information.
    pub fn about(&mut self) -> Result<Grid> {
        self.get(self.about_url())
    }

    /// Returns a grid describing what MIME types are available.
    pub fn formats(&mut self) -> Result<Grid> {
        self.get(self.formats_url())
    }

    /// Returns a grid of history data for a single point.
    pub fn his_read(&mut self, id: &Ref, range: &HisReadRange) -> Result<Grid> {
        let row = json!({
            "id": id.to_hayson(),
            "range": range.to_json_request_string()
        });
        let req_grid = Grid::new_internal(vec![row]);

        self.post(self.his_read_url(), &req_grid)
    }

    /// Writes boolean values to a single point.
    pub fn his_write_bool(
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

        self.post(self.his_write_url(), &req_grid)
    }

    /// Writes numeric values to a single point. `unit` must be a valid
    /// Haystack unit literal, such as `L/s` or `celsius`.
    pub fn his_write_num(
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

        self.post(self.his_write_url(), &req_grid)
    }

    /// Writes string values to a single point.
    pub fn his_write_str(
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

        self.post(self.his_write_url(), &req_grid)
    }

    /// Writes boolean values with UTC timestamps to a single point.
    /// `time_zone_name` must be a valid SkySpark timezone name.
    pub fn utc_his_write_bool(
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

        self.post(self.his_write_url(), &req_grid)
    }

    /// Writes numeric values with UTC timestamps to a single point.
    /// `unit` must be a valid Haystack unit literal, such as `L/s` or
    /// `celsius`.
    /// `time_zone_name` must be a valid SkySpark timezone name.
    pub fn utc_his_write_num(
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

        self.post(self.his_write_url(), &req_grid)
    }

    /// Writes string values with UTC timestamps to a single point.
    /// `time_zone_name` must be a valid SkySpark timezone name.
    pub fn utc_his_write_str(
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

        self.post(self.his_write_url(), &req_grid)
    }

    /// The Haystack nav operation.
    pub fn nav(&mut self, nav_id: Option<&Ref>) -> Result<Grid> {
        let req_grid = match nav_id {
            Some(nav_id) => {
                let row = json!({ "navId": nav_id.to_hayson() });
                Grid::new_internal(vec![row])
            }
            None => Grid::new_internal(Vec::new()),
        };

        self.post(self.nav_url(), &req_grid)
    }

    /// Returns a grid containing the operations available on the server.
    pub fn ops(&mut self) -> Result<Grid> {
        self.get(self.ops_url())
    }

    /// Returns a grid containing the records matching the given Axon
    /// filter string.
    pub fn read(&mut self, filter: &str, limit: Option<u64>) -> Result<Grid> {
        let row = match limit {
            Some(integer) => json!({"filter": filter, "limit": integer}),
            None => json!({ "filter": filter }),
        };

        let req_grid = Grid::new_internal(vec![row]);
        self.post(self.read_url(), &req_grid)
    }

    /// Returns a grid containing the records matching the given id
    /// `Ref`s.
    pub fn read_by_ids(&mut self, ids: &[Ref]) -> Result<Grid> {
        let rows = ids.iter().map(|id| json!({"id": id.to_hayson()})).collect();

        let req_grid = Grid::new_internal(rows);
        self.post(self.read_url(), &req_grid)
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
    pub fn eval(&mut self, axon_expr: &str) -> Result<Grid> {
        let row = json!({ "expr": axon_expr });
        let req_grid = Grid::new_internal(vec![row]);
        self.post(self.eval_url(), &req_grid)
    }
}

fn http_response_to_grid(res: reqwest::blocking::Response) -> Result<Grid> {
    let json: serde_json::Value = res.json()?;
    let grid: Grid = json.try_into()?;

    if grid.is_error() {
        Err(Error::Grid { err_grid: grid })
    } else {
        Ok(grid)
    }
}

#[cfg(test)]
mod test {
    use crate::api::HisReadRange;
    use crate::blocking::SkySparkClient;
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

    fn new_client() -> SkySparkClient {
        let username = username();
        let password = password();
        let reqwest_client = reqwest::blocking::Client::new();
        SkySparkClient::new_with_client(
            project_api_url(),
            &username,
            &password,
            reqwest_client,
        )
        .unwrap()
    }

    #[test]
    fn about() {
        let mut client = new_client();
        let grid = client.about().unwrap();
        assert_eq!(grid.rows()[0]["whoami"], json!(username()));
    }

    #[test]
    fn formats() {
        let mut client = new_client();
        let grid = client.formats().unwrap();
        assert!(grid.rows()[0]["dis"].is_string());
    }

    #[test]
    fn his_read_today() {
        let range = HisReadRange::Today;
        his_read(&range);
    }

    #[test]
    fn his_read_yesterday() {
        let range = HisReadRange::Yesterday;
        his_read(&range);
    }

    #[test]
    fn his_read_date() {
        let range =
            HisReadRange::Date(chrono::NaiveDate::from_ymd(2019, 1, 1).into());
        his_read(&range);
    }

    #[test]
    fn his_read_date_span() {
        let range = HisReadRange::DateSpan {
            start: chrono::NaiveDate::from_ymd(2019, 1, 1).into(),
            end: chrono::NaiveDate::from_ymd(2019, 1, 2).into(),
        };
        his_read(&range);
    }

    #[test]
    fn his_read_date_time_span() {
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
        his_read(&range);
    }

    #[test]
    fn his_read_date_time() {
        use chrono::DateTime;
        use chrono_tz::Australia::Sydney;

        let date_time =
            DateTime::parse_from_rfc3339("2012-10-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let range = HisReadRange::SinceDateTime {
            date_time: date_time.into(),
        };
        his_read(&range);
    }

    #[test]
    fn his_read_date_time_utc() {
        use chrono::DateTime;
        use chrono_tz::Etc::UTC;

        let date_time = DateTime::parse_from_rfc3339("2012-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&UTC);
        let range = HisReadRange::SinceDateTime {
            date_time: date_time.into(),
        };
        his_read(&range);
    }

    fn his_read(range: &HisReadRange) {
        let filter = format!("point and his and hisEnd");

        let mut client = new_client();
        let points_grid = client.read(&filter, Some(1)).unwrap();

        let point_ref = points_grid.rows()[0]["id"].as_hs_ref().unwrap();
        let his_grid = client.his_read(&point_ref, &range).unwrap();

        assert!(his_grid.meta()["hisStart"].is_hs_date_time());
        assert!(his_grid.meta()["hisEnd"].is_hs_date_time());
    }

    fn get_ref_for_filter(client: &mut SkySparkClient, filter: &str) -> Ref {
        let points_grid = client.read(filter, Some(1)).unwrap();
        let point_ref = points_grid.rows()[0]["id"].as_hs_ref().unwrap();
        point_ref
    }

    #[test]
    fn utc_his_write_bool() {
        use chrono::{DateTime, Duration, NaiveDateTime, Utc};

        let ndt = NaiveDateTime::parse_from_str(
            "2021-01-10 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        .unwrap();

        let date_time1 = DateTime::from_utc(ndt, Utc);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client();

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Bool\"",
        );
        let his_data = vec![
            (date_time1, false),
            (date_time2, false),
            (date_time3, false),
        ];

        let res = client
            .utc_his_write_bool(&id, "Sydney", &his_data[..])
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn his_write_bool() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let mut client = new_client();

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Bool\"",
        );
        let his_data = vec![
            (date_time1.into(), true),
            (date_time2.into(), false),
            (date_time3.into(), true),
        ];

        let res = client.his_write_bool(&id, &his_data[..]).unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn utc_his_write_num() {
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

        let mut client = new_client();

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and unit",
        )
       ;

        let unit = Some("L/s".to_owned());

        let his_data = vec![
            (date_time1, Number::new(111.111, unit.clone())),
            (date_time2, Number::new(222.222, unit.clone())),
            (date_time3, Number::new(333.333, unit.clone())),
        ];

        let res = client
            .utc_his_write_num(&id, "Sydney", &his_data[..])
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn his_write_num() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client();

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and unit",
        )
       ;

        let unit = Some("L/s".to_owned());

        let his_data = vec![
            (date_time1.into(), Number::new(10.0, unit.clone())),
            (date_time2.into(), Number::new(15.34, unit.clone())),
            (date_time3.into(), Number::new(1.234, unit.clone())),
        ];

        let res = client.his_write_num(&id, &his_data[..]).unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn utc_his_write_num_no_unit() {
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

        let mut client = new_client();

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and not unit",
        )
       ;
        let his_data = vec![
            (date_time1, Number::new_unitless(11.11)),
            (date_time2, Number::new_unitless(22.22)),
            (date_time3, Number::new_unitless(33.33)),
        ];

        let res = client
            .utc_his_write_num(&id, "Sydney", &his_data[..])
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn his_write_num_no_unit() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client();

        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Number\" and not unit",
        )
       ;

        let his_data = vec![
            (date_time1.into(), Number::new_unitless(10.0)),
            (date_time2.into(), Number::new_unitless(15.34)),
            (date_time3.into(), Number::new_unitless(1.234)),
        ];

        let res = client.his_write_num(&id, &his_data[..]).unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn utc_his_write_str() {
        use chrono::{DateTime, Duration, NaiveDateTime, Utc};

        let ndt = NaiveDateTime::parse_from_str(
            "2021-01-10 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        .unwrap();

        let date_time1 = DateTime::from_utc(ndt, Utc);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client();
        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Str\"",
        );

        let his_data = vec![
            (date_time1, "utc".to_owned()),
            (date_time2, "data".to_owned()),
            (date_time3, "here".to_owned()),
        ];

        let res = client
            .utc_his_write_str(&id, "Sydney", &his_data[..])
            .unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn his_write_str() {
        use chrono::{DateTime, Duration};
        use chrono_tz::Australia::Sydney;

        let date_time1 =
            DateTime::parse_from_rfc3339("2019-08-01T00:00:00+10:00")
                .unwrap()
                .with_timezone(&Sydney);
        let date_time2 = date_time1 + Duration::minutes(5);
        let date_time3 = date_time1 + Duration::minutes(10);

        let mut client = new_client();
        let id = get_ref_for_filter(
            &mut client,
            "continuousIntegrationHisWritePoint and kind == \"Str\"",
        );

        let his_data = vec![
            (date_time1.into(), "hello".to_owned()),
            (date_time2.into(), "world".to_owned()),
            (date_time3.into(), "!".to_owned()),
        ];

        let res = client.his_write_str(&id, &his_data[..]).unwrap();
        assert_eq!(res.rows().len(), 0);
    }

    #[test]
    fn nav_root() {
        let mut client = new_client();
        let grid = client.nav(None).unwrap();
        assert!(grid.rows()[0]["navId"].is_hs_ref());
    }

    #[test]
    fn nav() {
        let mut client = new_client();
        let root_grid = client.nav(None).unwrap();
        let child_nav_id = root_grid.rows()[0]["navId"].as_hs_ref().unwrap();

        let child_grid = client.nav(Some(&child_nav_id)).unwrap();
        let final_nav_id = child_grid.rows()[0]["navId"].as_hs_ref().unwrap();
        assert_ne!(child_nav_id, final_nav_id);
    }

    #[test]
    fn ops() {
        let mut client = new_client();
        let grid = client.ops().unwrap();
        assert!(grid.rows()[0]["name"].is_string());
    }

    #[test]
    fn read_with_no_limit() {
        let mut client = new_client();
        let grid = client.read("point", None).unwrap();

        assert!(grid.rows()[0]["id"].is_hs_ref());
        assert!(grid.rows().len() > 10);
    }

    #[test]
    fn read_with_zero_limit() {
        let mut client = new_client();
        let grid = client.read("id", Some(0)).unwrap();
        assert_eq!(grid.rows().len(), 0);
    }

    #[test]
    fn read_with_non_zero_limit() {
        let mut client = new_client();
        let grid = client.read("id", Some(1)).unwrap();
        assert_eq!(grid.rows().len(), 1);

        let grid = client.read("id", Some(3)).unwrap();
        assert_eq!(grid.rows().len(), 3);
    }

    #[test]
    fn read_by_ids_with_no_ids() {
        let mut client = new_client();
        let ids = vec![];
        let grid_result = client.read_by_ids(&ids);
        assert!(grid_result.is_err());
    }

    #[test]
    fn read_by_ids_single() {
        let mut client = new_client();
        // Get some valid ids:
        let grid1 = client.read("id", Some(1)).unwrap();
        let ref1 = grid1.rows()[0]["id"].as_hs_ref().unwrap().clone();
        let ids = vec![ref1];
        let grid2 = client.read_by_ids(&ids).unwrap();
        assert_eq!(grid1, grid2);
    }

    #[test]
    fn read_by_ids_multiple() {
        let mut client = new_client();
        // Get some valid ids:
        let grid1 = client.read("id", Some(2)).unwrap();
        let ref1 = grid1.rows()[0]["id"].as_hs_ref().unwrap().clone();
        let ref2 = grid1.rows()[1]["id"].as_hs_ref().unwrap().clone();

        let ids = vec![ref1, ref2];
        let grid2 = client.read_by_ids(&ids).unwrap();
        assert_eq!(grid1, grid2);
    }

    #[test]
    fn eval() {
        let mut client = new_client();
        let axon_expr = "readAll(id and mod)[0..1].keepCols([\"id\", \"mod\"])";
        let grid = client.eval(axon_expr).unwrap();
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

    #[test]
    fn error_grid() {
        use crate::err::Error;

        let mut client = new_client();
        let grid_result = client.eval("reabDDDAll(test");

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

    #[test]
    fn project_name_works() {
        let client = new_client();
        assert!(client.project_name().len() > 3);
    }

    #[test]
    fn recovers_from_invalid_auth_token() {
        let mut client = new_client();

        let bad_token = "badauthtoken";

        assert_ne!(client.test_auth_token(), bad_token);

        // Check the client works before modifying the auth token:
        let grid1 = client.about().unwrap();
        assert_eq!(grid1.rows()[0]["whoami"], json!(username()));

        client.test_manually_set_auth_token(bad_token);
        assert_eq!(client.test_auth_token(), bad_token);

        // Check the client still works after setting a bad auth token:
        let grid2 = client.about().unwrap();
        assert_eq!(grid2.rows()[0]["whoami"], json!(username()));
    }
}
