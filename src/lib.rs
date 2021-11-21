//! An asynchronous client library for rqlite.
//!
//! This library uses tokio for sockets and hyper to handle http requests.
//! 
//! Currently there is no transaction support.
//! ```
//! use rqlite::ConnectOptions;
//!
//! let mut conn = ConnectOptions::new("my.node.local", 4001)
//!     .scheme(Scheme::HTTPS)
//!     .user("root")
//!     .pass("root")
//!		.connect().await?;
//!	conn.execute("SELECT * FROM foo where id = ?;", par!(1)).await?;
//! ```

mod connect;
mod cursor;
mod row;
mod types;
mod error;

pub use connect::{Node, Scheme, ConnectOptions, Connection};
pub use serde_json::{Value, to_value};
pub use error::RqliteError;
