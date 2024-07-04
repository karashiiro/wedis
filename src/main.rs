mod commands;
mod connection;
mod database;
mod known_issues;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use connection::{Client, ClientError, Connection, ConnectionContext};
use database::Database;
use redcon::Conn;
use rocksdb::{Options, TransactionDB, DB};
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

fn handle_command(conn: &mut Conn, db: &Database, args: Vec<Vec<u8>>) {
    let mut conn = Client::new(conn);
    let name = String::from_utf8_lossy(&args[0]).to_uppercase();

    log_command(args.clone());
    match name.as_str() {
        "QUIT" => commands::quit(&mut conn),
        "PING" => commands::ping(&mut conn, &args),
        "ECHO" => commands::echo(&mut conn, &args),
        "CLIENT" => commands::client(&mut conn, &args),
        "SET" => handle_result(commands::set(&mut conn, db, &args)),
        "GET" => handle_result(commands::get(&mut conn, db, &args)),
        "INCR" => handle_result(commands::incr(&mut conn, db, &args)),
        "INCRBY" => handle_result(commands::incrby(&mut conn, db, &args)),
        "DECR" => handle_result(commands::decr(&mut conn, db, &args)),
        "DECRBY" => handle_result(commands::decrby(&mut conn, db, &args)),
        "DEL" => handle_result(commands::del(&mut conn, db, &args)),
        "UNLINK" => handle_result(commands::unlink(&mut conn, db, &args)),
        "EXISTS" => handle_result(commands::exists(&mut conn, db, &args)),
        "HSET" => handle_result(commands::hset(&mut conn, db, &args)),
        "HGET" => handle_result(commands::hget(&mut conn, db, &args)),
        "SELECT" => conn.write_string("OK"),
        "INFO" => commands::info(&mut conn, &args),
        _ => conn.write_error(ClientError::UnknownCommand),
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let path = ".wedis";
    {
        let db_raw = TransactionDB::open_default(path).expect("Failed to open database");
        let db = Arc::new(Mutex::new(Database::new(db_raw)));

        let mut s = redcon::listen("127.0.0.1:6379", db).expect("Failed to start server");
        s.opened = Some(|conn, db| {
            info!("Got new connection from {}", conn.addr());

            let connection_id = db.lock().unwrap().acquire_connection();
            conn.context = Some(Box::new(ConnectionContext::new(connection_id)));
        });
        s.closed = Some(|_conn, _db, err| {
            if let Some(err) = err {
                error!("{}", err)
            }
        });
        s.command = Some(|conn, db, args| handle_command(conn, &db.lock().unwrap(), args));
        info!("Serving at {}", s.local_addr());

        known_issues::warn_known_issues();

        s.serve().expect("Failed to execute server");
    }
    let _ = DB::destroy(&Options::default(), path);
}
