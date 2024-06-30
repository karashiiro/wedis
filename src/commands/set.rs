use anyhow::Result;
use redcon::Conn;
use rocksdb::DB;

pub fn set(conn: &mut Conn, db: &DB, args: &Vec<Vec<u8>>) -> Result<()> {
    if args.len() < 3 {
        conn.write_error("ERR wrong number of arguments");
        return Ok(());
    }
    db.put(args[1].to_owned(), args[2].to_owned())?;
    conn.write_string("OK");
    Ok(())
}
