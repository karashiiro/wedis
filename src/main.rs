#![feature(trait_alias)]

mod commands;
mod connection;
mod database;
mod indexing;
mod known_issues;
mod server;
mod time;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use connection::{Client, ClientError, Connection, ConnectionContext};
use database::Database;
use rocksdb::{Options, TransactionDB, DB};
use server::{Conn, Handler, ServerError};
use tokio::signal;
use tracing::{debug, error, info, Level};
use tracing_subscriber;

#[macro_use(concat_string)]
extern crate concat_string;

fn handle_result(result: Result<()>) {
    if let Err(err) = result {
        error!(cause = ?err, "command failed")
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
        "HELLO" => commands::hello(&mut conn, &args),
        "PING" => commands::ping(&mut conn, &args),
        "ECHO" => commands::echo(&mut conn, &args),
        "CLIENT" => commands::client(&mut conn, &args),
        "APPEND" => handle_result(commands::append(&mut conn, db, &args)),
        "SET" => handle_result(commands::set(&mut conn, db, &args)),
        "SETEX" => handle_result(commands::setex(&mut conn, db, &args)),
        "SETNX" => handle_result(commands::setnx(&mut conn, db, &args)),
        "SETRANGE" => handle_result(commands::setrange(&mut conn, db, &args)),
        "GET" => handle_result(commands::get(&mut conn, db, &args)),
        "MGET" => handle_result(commands::mget(&mut conn, db, &args)),
        "GETRANGE" => handle_result(commands::getrange(&mut conn, db, &args)),
        "GETDEL" => handle_result(commands::getdel(&mut conn, db, &args)),
        "GETSET" => handle_result(commands::getset(&mut conn, db, &args)),
        "STRLEN" => handle_result(commands::strlen(&mut conn, db, &args)),
        "SUBSTR" => handle_result(commands::substr(&mut conn, db, &args)),
        "INCR" => handle_result(commands::incr(&mut conn, db, &args)),
        "INCRBY" => handle_result(commands::incrby(&mut conn, db, &args)),
        "INCRBYFLOAT" => handle_result(commands::incrbyfloat(&mut conn, db, &args)),
        "DECR" => handle_result(commands::decr(&mut conn, db, &args)),
        "DECRBY" => handle_result(commands::decrby(&mut conn, db, &args)),
        "DEL" => handle_result(commands::del(&mut conn, db, &args)),
        "UNLINK" => handle_result(commands::unlink(&mut conn, db, &args)),
        "EXISTS" => handle_result(commands::exists(&mut conn, db, &args)),
        "EXPIRE" => handle_result(commands::expire(&mut conn, db, &args)),
        "PEXPIRE" => handle_result(commands::pexpire(&mut conn, db, &args)),
        "EXPIREAT" => handle_result(commands::expireat(&mut conn, db, &args)),
        "PEXPIREAT" => handle_result(commands::pexpireat(&mut conn, db, &args)),
        "EXPIRETIME" => handle_result(commands::expiretime(&mut conn, db, &args)),
        "PEXPIRETIME" => handle_result(commands::pexpiretime(&mut conn, db, &args)),
        "PERSIST" => handle_result(commands::persist(&mut conn, db, &args)),
        "TTL" => handle_result(commands::ttl(&mut conn, db, &args)),
        "PTTL" => handle_result(commands::pttl(&mut conn, db, &args)),
        "HSET" => handle_result(commands::hset(&mut conn, db, &args)),
        "HGET" => handle_result(commands::hget(&mut conn, db, &args)),
        "HSTRLEN" => handle_result(commands::hstrlen(&mut conn, db, &args)),
        "BITCOUNT" => handle_result(commands::bitcount(&mut conn, db, &args)),
        "BITPOS" => handle_result(commands::bitpos(&mut conn, db, &args)),
        "GETBIT" => handle_result(commands::getbit(&mut conn, db, &args)),
        "SETBIT" => handle_result(commands::setbit(&mut conn, db, &args)),
        "SELECT" => conn.write_string("OK"),
        "INFO" => commands::info(&mut conn, &args),
        "TIME" => handle_result(commands::time(&mut conn)),
        _ => {
            error!("Unknown command: {}", name);
            conn.write_error(ClientError::UnknownCommand)
        }
    }
}

#[derive(Clone)]
struct MyHandler(Arc<Mutex<Database>>);

impl Handler for MyHandler {
    async fn on_open(&self, conn: &mut Conn) {
        info!("Got new connection from {}", conn.addr());

        let connection_id = self.0.lock().unwrap().acquire_connection();
        conn.context = Some(Box::new(ConnectionContext::new(connection_id)));
    }

    async fn on_close(&self, _conn: &mut Conn, err: Option<&ServerError>) {
        if let Some(err) = err {
            error!(cause = ?err, "connection aborted")
        }
    }

    async fn on_command(&self, conn: &mut Conn, args: Vec<Vec<u8>>) {
        handle_command(conn, &self.0.lock().unwrap(), args);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    known_issues::warn_known_issues();

    let path = ".wedis";
    {
        let db_raw = TransactionDB::open_default(path).expect("Failed to open database");
        let db = Arc::new(Mutex::new(Database::new(db_raw)));

        let h = MyHandler(db);
        server::serve(h, signal::ctrl_c()).await?;
    }
    let _ = DB::destroy(&Options::default(), path);

    Ok(())
}
