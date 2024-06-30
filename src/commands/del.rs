use std::{collections::HashMap, sync::Mutex};

use redcon::Conn;

pub fn del(conn: &mut Conn, db: &Mutex<HashMap<Vec<u8>, Vec<u8>>>, args: &Vec<Vec<u8>>) {
    if args.len() != 2 {
        conn.write_error("ERR wrong number of arguments");
        return;
    }
    let mut db = db.lock().unwrap();
    match db.remove(&args[1]) {
        Some(_) => conn.write_integer(1),
        None => conn.write_integer(0),
    }
}
