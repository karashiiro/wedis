use anyhow::Result;

use crate::{connection::Connection, database::DatabaseOperations};

#[tracing::instrument(skip_all)]
pub fn del(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    // TODO: Handle multiple values
    if args.len() != 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return Ok(());
    }

    let n_fields = db.delete(&args[1])?;
    conn.write_integer(n_fields);

    Ok(())
}
