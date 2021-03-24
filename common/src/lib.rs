pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

pub fn new_arc_rwlock<T>(t: T) -> Arc<RwLock<T>> {
    Arc::new(RwLock::new(t))
}

pub fn new_arc_mutex<T>(t: T) -> Arc<Mutex<T>> {
    Arc::new(Mutex::new(t))
}

#[derive(Debug, Clone)]
pub enum Item {
    JSON(Value),
    Default(String),
}

impl<'a> From<&'a str> for Item {
    fn from(str: &'a str) -> Self {
        match Item::is_valid_json(str) {
            Ok((is, obj)) if is => {
                return Item::JSON(obj);
            }
            _ => Item::Default(str.to_string()),
        }
    }
}

impl Item {
    pub fn is_valid_json(str: &str) -> Result<(bool, Value)> {
        match serde_json::from_str::<Value>(str) {
            Ok(value) => Ok((true, value)),
            Err(_) => Ok((false, Value::Null)),
        }
    }

    pub fn is_json(&self) -> bool {
        match *self {
            Item::JSON(_) => true,
            Item::Default(_) => false,
        }
    }

    pub fn string(&self) -> String {
        match self {
            Item::JSON(_str) => _str.to_string(),
            Item::Default(_str) => _str.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let _str = r#"
        {
          "name": "lijiming",
          "age": 88,
          "likes": "oldbaby",
          "addr": null
        }"#;

        let item = Item::from(_str);
        if !item.is_json() {
            panic!(r#"not expect json object"#)
        }
    }
}
