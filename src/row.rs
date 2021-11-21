use serde::de::DeserializeOwned;
use serde_json::Value;
use std::error::Error;
use std::io::ErrorKind;

#[derive(Debug)]
pub struct Row {
    row: Vec<Value>
}

/// SQL row
impl Row {
    pub(crate) fn new(row: Vec<Value>) -> Row {
        Row { row }
    }

    /// Get n element in row
    /// Return error if element cannot be formatted
    pub fn get<T: DeserializeOwned>(&self, id: usize) -> Result<T, Box<dyn Error>> {
        if id >= self.row.len() {
            return Err(Box::new(std::io::Error::new(ErrorKind::NotFound, format!("Row element with id {} doesn't exist", id))));
        }

        let val: T = serde_json::from_value(self.row[id].clone())?;
        Ok(val)
    }
}
