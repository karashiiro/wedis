use anyhow::Result;
use redcon::Conn;

use crate::database::{Database, DatabaseOperations};

#[tracing::instrument(skip_all)]
pub fn del(conn: &mut Conn, db: &Database, args: &Vec<Vec<u8>>) -> Result<()> {
    // TODO: Handle multiple values
    if args.len() != 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    let n_fields = db.delete(&args[1])?;
    conn.write_integer(n_fields);

    Ok(())
}
