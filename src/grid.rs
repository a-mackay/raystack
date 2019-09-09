use crate::hsref::Ref;
use crate::{is_tag_name, TagName};
use serde_json::json;
use serde_json::map::Map;
use serde_json::Value;
use serde_json::{to_string, to_string_pretty};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::convert::TryInto;
use std::iter::FromIterator;

/// A wrapper around a `serde_json::Value` which represents a Haystack Grid.
/// Columns will always be sorted in alphabetical order.
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

        for &key in &sorted_keys {
            if !is_tag_name(key) {
                return Err(ParseJsonGridError::new(format!(
                    "Column name '{}' is not a valid tag name",
                    key
                )));
            }
        }

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

    /// Add a new column, or overwrite an existing column by mapping
    /// each row to a new cell value.
    pub fn add_col<F>(&mut self, col_name: TagName, f: F)
    where
        F: Fn(&mut Map<String, Value>) -> Value,
    {
        let col_name = col_name.to_string();

        for row in self.json["rows"]
            .as_array_mut()
            .expect("rows is a JSON Array")
        {
            let row = row.as_object_mut().expect("Each row is a JSON object");
            let value = f(row);
            row.insert(col_name.clone(), value);
        }

        let mut new_col_names: HashSet<&str> =
            HashSet::from_iter(self.col_name_strs());
        new_col_names.insert(&col_name);
        let mut new_col_names = new_col_names.into_iter().collect::<Vec<_>>();
        new_col_names.sort();
        let new_col_objects =
            new_col_names.iter().map(|c| json!({ "name": c })).collect();
        self.json["cols"] = Value::Array(new_col_objects);
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
    pub fn col_names(&self) -> Vec<TagName> {
        self.cols()
            .iter()
            .map(|col| {
                let name =
                    col["name"].as_str().expect("col name is a JSON string");
                TagName::new(name.to_owned())
                    .expect("col names in grid are valid tag names")
            })
            .collect()
    }

    /// Return a vector containing the column names in this grid, as strings.
    pub fn col_name_strs(&self) -> Vec<&str> {
        self.cols()
            .iter()
            .map(|col| col["name"].as_str().expect("col name is a JSON string"))
            .collect()
    }

    /// Return a vector containing the values in the given column.
    pub fn col_to_vec(&self, col_name: &str) -> Vec<Option<&Value>> {
        self.rows().iter().map(|row| row.get(col_name)).collect()
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

    /// Sort the rows with a comparator function. This sort is stable.
    pub fn sort_rows<F>(&mut self, compare: F)
    where
        F: FnMut(&Value, &Value) -> Ordering,
    {
        let rows = self.json["rows"]
            .as_array_mut()
            .expect("rows is a JSON Array");
        rows.sort_by(compare);
    }

    /// Return the number of rows in the grid.
    pub fn size(&self) -> usize {
        self.rows().len()
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

        let col_names = self.col_name_strs();
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

#[cfg(test)]
mod test {
    use super::Grid;
    use crate::TagName;
    use serde_json::json;

    #[test]
    fn add_col() {
        let rows = vec![
            json!({"id": "abcd1234", "dis": "Hello World"}),
            json!({"id": "cdef5678", "dis": "Hello Kitty"}),
        ];
        let mut grid = Grid::new(rows).unwrap();

        let new_col = TagName::new("newCol".to_owned()).unwrap();

        grid.add_col(new_col, |row| {
            let id = row["id"].as_str().unwrap();
            let dis = row["dis"].as_str().unwrap();
            json!(id.to_string() + dis)
        });

        assert_eq!(
            grid.rows()[0]["newCol"].as_str().unwrap(),
            "abcd1234Hello World"
        );
        assert_eq!(
            grid.rows()[1]["newCol"].as_str().unwrap(),
            "cdef5678Hello Kitty"
        );
        assert!(grid.to_string().contains("abcd1234Hello World"));
        assert!(grid.to_string().contains("cdef5678Hello Kitty"));
        assert!(grid
            .col_names()
            .contains(&TagName::new("newCol".to_owned()).unwrap()));

        println!("{}", grid.to_csv_string().unwrap());
    }

    #[test]
    fn add_col_overwrite_existing_col() {
        let rows = vec![
            json!({"id": "abcd1234", "dis": "Hello World"}),
            json!({"id": "cdef5678", "dis": "Hello Kitty"}),
        ];
        let mut grid = Grid::new(rows).unwrap();

        let col_name = TagName::new("dis".to_owned()).unwrap();

        grid.add_col(col_name, |row| {
            let id = row["id"].as_str().unwrap();
            let dis = row["dis"].as_str().unwrap();
            json!(id.to_string() + dis)
        });

        assert_eq!(
            grid.rows()[0]["dis"].as_str().unwrap(),
            "abcd1234Hello World"
        );
        assert_eq!(
            grid.rows()[1]["dis"].as_str().unwrap(),
            "cdef5678Hello Kitty"
        );
        assert!(grid.to_string().contains("abcd1234Hello World"));
        assert!(grid.to_string().contains("cdef5678Hello Kitty"));
        assert_eq!(grid.col_names().len(), 2);

        println!("{}", grid.to_csv_string().unwrap());
    }

    #[test]
    fn col_to_vec() {
        let rows = vec![
            json!({"id": "a"}),
            json!({"different": "thing"}),
            json!({"id": "b"}),
            json!({"id": "c"}),
        ];
        let grid = Grid::new(rows).unwrap();
        let col = grid.col_to_vec("id");

        assert_eq!(col[0].unwrap().as_str().unwrap(), "a");
        assert!(col[1].is_none());
        assert_eq!(col[2].unwrap().as_str().unwrap(), "b");
        assert_eq!(col[3].unwrap().as_str().unwrap(), "c");
    }

    #[test]
    fn sort_rows() {
        let rows = vec![
            json!({"id": "b"}),
            json!({"id": "d"}),
            json!({"id": "a"}),
            json!({"id": "c"}),
        ];
        let mut grid = Grid::new(rows).unwrap();

        {
            let original_cols = grid
                .col_to_vec("id")
                .into_iter()
                .map(|elem| elem.unwrap().as_str().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(original_cols, vec!["b", "d", "a", "c"]);
        }

        grid.sort_rows(|row1, row2| {
            let str1 = row1["id"].as_str().unwrap();
            let str2 = row2["id"].as_str().unwrap();
            str1.cmp(str2)
        });

        let new_cols = grid
            .col_to_vec("id")
            .into_iter()
            .map(|elem| elem.unwrap().as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(new_cols, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn bad_col_name() {
        let rows = vec![json!({"id": "d"}), json!({"BadTagName": "b"})];
        let grid = Grid::new(rows);
        assert!(grid.is_err());
    }

    #[test]
    fn size() {
        let empty_grid = Grid::new(vec![]).unwrap();
        assert_eq!(empty_grid.size(), 0);
        let rows = vec![json!({"id": "1"}), json!({"id": "2"})];
        let grid = Grid::new(rows).unwrap();
        assert_eq!(grid.size(), 2);
    }
}
