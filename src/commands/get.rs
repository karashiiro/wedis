use anyhow::Result;
use redcon::Conn;
use rocksdb::DB;

#[tracing::instrument(skip_all)]
pub fn get(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    // TODO: Error if value does not represent a string
    // "WRONGTYPE Operation against a key holding the wrong kind of value"
    match db.get(&args[1])? {
        Some(val) => Ok(conn.write_bulk(&val)),
        None => Ok(conn.write_null()),
    }
}
