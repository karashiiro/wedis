use anyhow::Result;
use redcon::Conn;

use crate::database::{Database, DatabaseError, DatabaseOperations};

#[tracing::instrument(skip_all)]
pub fn set(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 3 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    db.put_string(&args[1], &args[2])?;

    conn.write_string("OK");
    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn get(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    match db.get_string(&args[1]) {
        Ok(value) => match value {
            Some(val) => Ok(conn.write_bulk(&val)),
            None => Ok(conn.write_null()),
        },
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn
                .write_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        }
        Err(err) => Err(err.into()),
    }
}
