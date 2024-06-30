use std::{collections::HashMap, sync::Mutex};

use redcon::Conn;

pub fn set(conn: &mut Conn, db: &Mutex<HashMap<Vec<u8>, Vec<u8>>>, args: &Vec<Vec<u8>>) {
    if args.len() < 3 {
        conn.write_error("ERR wrong number of arguments");
        return;
    }
    let mut db = db.lock().unwrap();
    db.insert(args[1].to_owned(), args[2].to_owned());
    conn.write_string("OK");
}
