use std::collections::HashMap;

use anyhow::Result;
use redcon::Conn;
use rocksdb::DB;

#[tracing::instrument(skip_all)]
pub fn hget(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
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
