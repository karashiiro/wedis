use std::collections::HashMap;

use anyhow::Result;
use redcon::Conn;

use crate::database::{Database, DatabaseOperations};

#[tracing::instrument(skip_all)]
pub fn hset(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 4 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    // TODO: Avoid relying on encoding values as UTF-8 strings
    // TODO: Handle multiple values
    let key = &args[1];
    let subkey1 = String::from_utf8_lossy(&args[2]).into_owned();
    let subvalue1 = String::from_utf8_lossy(&args[3]).into_owned();

    let mut dict = HashMap::new();
    dict.insert(subkey1, subvalue1);

    // TODO: Error if existing value does not represent a hash
    // "WRONGTYPE Operation against a key holding the wrong kind of value"
    let value = serde_json::to_string(&dict)?;
    db.put(&key, value.as_bytes())?;

    conn.write_integer(1);
    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn hget(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 3 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    let key = &args[1];
    match db.get(key)? {
        Some(value) => {
            let value = String::from_utf8_lossy(&value);
            let dict: HashMap<String, String> = serde_json::from_str(&value)?;

            let subkey = String::from_utf8_lossy(&args[2]).into_owned();
            let value = dict.get(&subkey);

            // TODO: Error if value does not represent a hash
            // "WRONGTYPE Operation against a key holding the wrong kind of value"
            match value {
                Some(value) => Ok(conn.write_bulk(value.as_bytes())),
                None => Ok(conn.write_null()),
            }
        }
        None => Ok(conn.write_null()),
    }
}
