use std::collections::HashMap;

use anyhow::Result;
use redcon::Conn;
use rocksdb::DB;

#[tracing::instrument(skip_all)]
pub fn hset(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 4 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    // TODO: Handle multiple values
    let key = args[1].to_owned();
    let subkey1 = String::from_utf8_lossy(&args[2]).into_owned();
    let subvalue1 = String::from_utf8_lossy(&args[3]).into_owned();

    let mut dict = HashMap::new();
    dict.insert(subkey1, subvalue1);

    // TODO: Error if existing value does not represent a hash
    // "WRONGTYPE Operation against a key holding the wrong kind of value"
    let value = serde_json::to_string(&dict)?;
    db.put(key, value.as_bytes())?;

    conn.write_integer(1);
    Ok(())
}
