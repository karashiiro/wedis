use anyhow::Result;
use redcon::Conn;

use crate::database::{Database, DatabaseError, DatabaseOperations};

#[tracing::instrument(skip_all)]
pub fn hset(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 4 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    // TODO: Handle multiple values
    match db.put_hash_field(&args[1], &args[2], &args[3]) {
        Ok(()) => {
            conn.write_integer(1);
            Ok(())
        }
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn
                .write_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        }
        Err(err) => Err(err.into()),
    }
}

#[tracing::instrument(skip_all)]
pub fn hget(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 3 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    match db.get_hash_field(&args[1], &args[2]) {
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
