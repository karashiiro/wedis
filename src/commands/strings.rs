use anyhow::Result;
use redcon::Conn;
use rocksdb::DB;

use crate::types::{RedisString, RedisType, RedisValue};

#[tracing::instrument(skip_all)]
pub fn set(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 3 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    let redis_string: RedisValue<RedisString> = RedisString::new(&args[2]).into();
    let redis_string: Vec<u8> = redis_string.as_vec();
    db.put(args[1].to_owned(), redis_string)?;

    conn.write_string("OK");
    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn get(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    match db.get(&args[1])? {
        Some(val) => {
            // TODO: There's something wrong with this data model if we need to assume our data is correct first
            let redis_string: RedisValue<RedisString> = RedisValue::try_from_bytes(&val)?;
            if !redis_string.is_type(RedisType::String) {
                return Ok(conn.write_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }

            let redis_string: Vec<u8> = redis_string.as_vec();
            Ok(conn.write_bulk(&redis_string))
        }
        None => Ok(conn.write_null()),
    }
}
