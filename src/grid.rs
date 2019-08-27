use crate::hsref::Ref;
use serde_json::json;
use serde_json::map::Map;
use serde_json::Value;
use serde_json::{to_string, to_string_pretty};
use std::collections::HashSet;
use std::convert::TryInto;

/// A wrapper around a `serde_json::Value` which represents a Haystack Grid.
/// Columns will be sorted.
#[derive(Clone, Debug, PartialEq)]
pub struct Grid {
    json: Value,
}

impl Grid {
    /// Create a new `Grid` from rows. Each row must be a JSON Object.
    /// # Example
    /// ```rust
    /// use raystack::Grid;
    /// use serde_json::json;
    ///
    /// let row = json!({"firstName": "Otis", "lastName": "Jackson Jr."});
    /// let rows = vec![row];
    /// let grid = Grid::new(rows).unwrap();
    /// assert_eq!(grid.rows()[0]["firstName"], "Otis");
    /// ```
    pub fn new(rows: Vec<Value>) -> Result<Self, ParseJsonGridError> {
        let mut keys = HashSet::new();
        for row in &rows {
            if let Some(row_object) = row.as_object() {
                keys.extend(row_object.keys());
            } else {
                return Err(ParseJsonGridError::new(format!(
                    "Expected a JSON object for row but found {}",
                    row
                )));
            }
        }

        let mut sorted_keys = keys.into_iter().collect::<Vec<_>>();
        sorted_keys.sort();

        let cols = Value::Array(
            sorted_keys
                .iter()
                .map(|key| json!({ "name": key }))
                .collect(),
        );

        let mut json_grid = json!({
            "meta": {"ver": "3.0"},
        });

        let json_grid_insert =
            json_grid.as_object_mut().expect("grid is a JSON Object");
        let rows = Value::Array(rows);
        json_grid_insert.insert("cols".to_owned(), cols);
        json_grid_insert.insert("rows".to_owned(), rows);

        json_grid.try_into()
    }

    pub(crate) fn new_internal(rows: Vec<Value>) -> Self {
        Self::new(rows)
            .expect("creating grids within this crate should never fail")
    }

    /// Return a map which represents the metadata for the grid.
    pub fn meta(&self) -> &Map<String, Value> {
        &self.json["meta"]
            .as_object()
            .expect("meta is a JSON Object")
    }

    /// Return an owned map, which represents the
    /// metadata for the grid.
    pub fn to_meta(&self) -> Map<String, Value> {
        let meta = self.json["meta"]
            .as_object()
            .expect("meta is a JSON Object");
        meta.clone()
    }

    pub(crate) fn add_ref_to_meta(&mut self, hsref: &Ref) {
        let meta = self.json["meta"]
            .as_object_mut()
            .expect("meta is a JSON Object");
        meta.insert(
            "id".to_owned(),
            Value::String(hsref.to_encoded_json_string()),
        );
    }

    /// Return a vector of JSON values which represent the columns of the grid.
    pub fn cols(&self) -> &Vec<Value> {
        &self.json["cols"].as_array().expect("cols is a JSON Array")
    }

    /// Return a vector of owned JSON values which
    /// represent the columns of the grid.
    pub fn to_cols(&self) -> Vec<Value> {
        self.json["cols"]
            .as_array()
            .expect("cols is a JSON Array")
            .to_vec()
    }

    /// Return a vector containing the column names in this grid.
    pub fn col_names(&self) -> Vec<&str> {
        self.cols()
            .iter()
            .map(|col| col["name"].as_str().expect("col name is a JSON string"))
            .collect()
    }

    /// Return a vector of JSON values which represent the rows of the grid.
    pub fn rows(&self) -> &Vec<Value> {
        &self.json["rows"].as_array().expect("rows is a JSON Array")
    }

    /// Return a vector of owned JSON values which
    /// represent the rows of the grid.
    pub fn to_rows(&self) -> Vec<Value> {
        self.json["rows"]
            .as_array()
            .expect("rows is a JSON Array")
            .to_vec()
    }

    /// Return the string representation of the underlying JSON value.
    pub fn to_string(&self) -> String {
        to_string(&self.json)
            .expect("serializing grid to String should never fail")
    }

    /// Return a pretty formatted string representing the underlying JSON value.
    pub fn to_string_pretty(&self) -> String {
        to_string_pretty(&self.json)
            .expect("serializing grid to String should never fail")
    }

    /// Returns true if the grid appears to be an error grid.
    pub fn is_error(&self) -> bool {
        if let Some(err_val) = self.meta().get("err") {
            if let Some(err_str) = err_val.as_str() {
                err_str == MARKER_LITERAL
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Return the error trace if present.
    pub fn error_trace(&self) -> Option<String> {
        self.meta()["errTrace"].as_str().map(|s| s.to_owned())
    }

    /// Return a string containing a CSV representation of the grid.
    /// Nested structures such as Dicts (JSON objects) or Lists (JSON arrays)
    /// will not be expanded, and will be displayed as `<StructureType>`.
    pub fn to_csv_string(&self) -> Result<String, crate::Error> {
        let mut writer = csv::Writer::from_writer(vec![]);

        let col_names = self.col_names();
        writer.write_record(&col_names)?;

        for row in self.rows() {
            let mut row_values = Vec::new();
            for &col_name in &col_names {
                let value_string = match &row[col_name] {
                    Value::Array(_) => "<Array>".to_owned(),
                    Value::Bool(true) => "T".to_owned(),
                    Value::Bool(false) => "F".to_owned(),
                    Value::Null => "".to_owned(),
                    Value::Number(n) => n.to_string(),
                    Value::Object(_) => "<Object>".to_owned(),
                    Value::String(s) => s.to_owned(),
                };
                row_values.push(value_string);
            }
            writer.write_record(row_values)?;
        }

        match writer.into_inner() {
            Ok(bytes) => Ok(String::from_utf8(bytes)
                .expect("Bytes should be UTF8 since all input was UTF8")),
            Err(err) => {
                let io_err = err.error();
                let msg = io_err.to_string();
                Err(crate::Error::new_io(msg))
            }
        }
    }
}

const MARKER_LITERAL: &str = "m:";

impl std::convert::TryFrom<Value> for Grid {
    type Error = ParseJsonGridError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if !value["meta"].is_object() {
            return Err(ParseJsonGridError::new(
                "Could not find a JSON object for 'meta'".to_owned(),
            ));
        };

        let cols = value["cols"].as_array().ok_or_else(|| {
            ParseJsonGridError::new(
                "Could not find a JSON array for 'cols'".to_owned(),
            )
        })?;

        let rows = value["rows"].as_array().ok_or_else(|| {
            ParseJsonGridError::new(
                "Could not find a JSON array for 'rows'".to_owned(),
            )
        })?;

        for col in cols {
            if !col.is_object() {
                return Err(ParseJsonGridError::new(format!(
                    "Expected a JSON object for col but found {}",
                    col
                )));
            }
        }

        for row in rows {
            if !row.is_object() {
                return Err(ParseJsonGridError::new(format!(
                    "Expected a JSON object for row but found {}",
                    row
                )));
            }
        }

        Ok(Grid { json: value })
    }
}

/// Error denoting that a JSON value could not be parsed into a `Grid`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseJsonGridError {
    pub(crate) msg: String,
}

impl ParseJsonGridError {
    fn new(msg: String) -> Self {
        ParseJsonGridError { msg }
    }
}

impl std::error::Error for ParseJsonGridError {}

impl std::fmt::Display for ParseJsonGridError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}
