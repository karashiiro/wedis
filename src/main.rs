mod commands;
mod known_issues;

use std::collections::HashMap;
use std::sync::Mutex;
use tracing::{debug, error, info, Level};
use tracing_subscriber;

#[macro_use(concat_string)]
extern crate concat_string;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let db: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());

    let mut s = redcon::listen("127.0.0.1:6379", db).unwrap();
    s.opened = Some(|conn, _db| info!("Got new connection from {}", conn.addr()));
    s.closed = Some(|_conn, _db, err| {
        if let Some(err) = err {
            error!("{}", err)
        }
    });
    s.command = Some(|conn, db, args| {
        let name = String::from_utf8_lossy(&args[0]).to_uppercase();

        info!("Received command: \"{}\"", name);
        match name.as_str() {
            "PING" => conn.write_string("PONG"),
            "CLIENT" => {
                let mut parsed_args: Vec<String> = vec![];
                for arg in &args {
                    parsed_args.push(String::from_utf8_lossy(&arg).into_owned())
                }
                debug!("{:?}", parsed_args);
                conn.write_string("OK");
            }
            "SET" => commands::set(conn, db, &args),
            "GET" => commands::get(conn, db, &args),
            "DEL" => commands::del(conn, db, &args),
            "SELECT" => conn.write_string("OK"),
            "INFO" => commands::info(conn),
            _ => conn.write_error("ERR unknown command"),
        }
    });
    info!("Serving at {}", s.local_addr());

    known_issues::warn_known_issues();

    s.serve().unwrap();
}
