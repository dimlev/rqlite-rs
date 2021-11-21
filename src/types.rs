use serde::{Deserialize, Deserializer, de};

/// Sqlite types
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum Type {
    Null,
    Integer,
    Real,
    Text,
    Blob
}

/// Parse vector of json values
pub fn parse_vec_types<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Vec<Type>>, D::Error> {
    let s: Option<Vec<String>> = Option::deserialize(deserializer)?;
    match s {
        Some(s_vec) => {
            let mut vec: Vec<Type> = Vec::with_capacity(s_vec.len());
            for i in 0..s_vec.len() {
                vec.push(match get_type(s_vec[i].as_str()) {
                    Ok(val) => val,
                    Err(_) => return Err(de::Error::custom(format!("Unknewn sqlite type {}", s_vec[i])))
                });
            }
            Ok(Some(vec))
        }
        None => Ok(None)
    }
}

/// Intepret type
fn get_type(_type: &str) -> Result<Type, ()> {
    Ok(match _type {
        "" => Type::Null,
        "integer" => Type::Integer,
        "real" => Type::Real,
        "text" => Type::Text,
        "blob" => Type::Blob,
        _ => return Err(())
    })
}

/// Specify parameters for parameterized statements.
///
/// Warning: Using raw queries may introduce vulnerabilities.
///
/// Named parameters is not supported by rqlite.
/// ```
/// conn.execute("SELECT * FROM foo where name = ?", par!("fiona"))?;
/// ```
#[macro_export]
macro_rules! par {
    ( $( $x:expr ),* ) => {
        {
            let mut vec: Vec<$crate::Value> = Vec::new();
            $(
                vec.push($crate::to_value($x)?);
            )*
            vec
        }
    };
}
