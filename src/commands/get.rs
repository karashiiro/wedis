use std::{collections::HashMap, sync::Mutex};

use redcon::Conn;

pub fn get(conn: &mut Conn, db: &Mutex<HashMap<Vec<u8>, Vec<u8>>>, args: &Vec<Vec<u8>>) {
    if args.len() < 2 {
        conn.write_error("ERR wrong number of arguments");
        return;
    }
    let db = db.lock().unwrap();
    match db.get(&args[1]) {
        Some(val) => conn.write_bulk(val),
        None => conn.write_null(),
    }
}
