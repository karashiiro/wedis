use anyhow::Result;
use redcon::Conn;
use rocksdb::{ErrorKind, DB};

#[tracing::instrument(skip_all)]
pub fn del(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() != 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }
    match db.delete(&args[1]) {
        Ok(_) => Ok(conn.write_integer(1)),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(conn.write_integer(0)),
            _ => Err(err.into()),
        },
    }
}
