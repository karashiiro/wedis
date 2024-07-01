#![feature(trait_alias)]

mod commands;
mod connection;
mod known_issues;
mod types;

use anyhow::Result;
use connection::ConnectionContext;
use redcon::Conn;
use rocksdb::{Options, DB};
use tracing::{debug, error, info, Level};
use tracing_subscriber;

#[macro_use(concat_string)]
extern crate concat_string;

fn handle_result(result: Result<()>) {
    if let Err(err) = result {
        error!("{}", err)
    }
}

fn log_command(args: Vec<Vec<u8>>) {
    let mut parsed_args: Vec<String> = vec![];
    for arg in args {
        parsed_args.push(String::from_utf8_lossy(&arg).into_owned())
    }
    debug!("> {:?}", parsed_args);
}

fn handle_command(conn: &mut Conn, db: &DB, args: Vec<Vec<u8>>) {
    let name = String::from_utf8_lossy(&args[0]).to_uppercase();

    log_command(args.clone());
    match name.as_str() {
        "PING" => conn.write_string("PONG"),
        "CLIENT" => commands::client(conn, db, &args),
        "SET" => handle_result(commands::set(conn, db, &args)),
        "GET" => handle_result(commands::get(conn, db, &args)),
        "DEL" => handle_result(commands::del(conn, db, &args)),
        "HSET" => handle_result(commands::hset(conn, db, &args)),
        "HGET" => handle_result(commands::hget(conn, db, &args)),
        "SELECT" => conn.write_string("OK"),
        "INFO" => commands::info(conn),
        _ => conn.write_error("ERR unknown command"),
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let path = ".wedis";
    {
        let db = DB::open_default(path).unwrap();

        let mut s = redcon::listen("127.0.0.1:6379", db).unwrap();
        s.opened = Some(|conn, _db| {
            info!("Got new connection from {}", conn.addr());
            conn.context = Some(Box::new(ConnectionContext::new()));
        });
        s.closed = Some(|_conn, _db, err| {
            if let Some(err) = err {
                error!("{}", err)
            }
        });
        s.command = Some(handle_command);
        info!("Serving at {}", s.local_addr());

        known_issues::warn_known_issues();

        s.serve().unwrap();
    }
    let _ = DB::destroy(&Options::default(), path);
}
