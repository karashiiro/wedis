use std::any::Any;

use redcon::Conn;
use thiserror::Error;

#[cfg(test)]
use mockall::automock;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("ERR no context")]
    NoContext,
    #[error("ERR unknown command")]
    UnknownCommand,
    #[error("ERR unknown attribute")]
    UnknownAttribute,
    #[error("ERR wrong number of arguments for command")]
    ArgCount,
    #[error("NX and XX, GT or LT options at the same time are not compatible")]
    ExpireNxOptions,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}

pub struct ConnectionContext {
    id: i64,
    lib_name: String,
    lib_version: String,
    connection_name: Option<String>,
}

impl ConnectionContext {
    pub fn new(id: i64) -> Self {
        ConnectionContext {
            id,
            lib_name: "".to_string(),
            lib_version: "".to_string(),
            connection_name: None,
        }
    }

    pub fn set_lib_name(&mut self, lib_name: &str) {
        self.lib_name = lib_name.to_owned()
    }

    pub fn set_lib_version(&mut self, lib_version: &str) {
        self.lib_version = lib_version.to_owned()
    }

    pub fn set_connection_name(&mut self, connection_name: &str) {
        self.connection_name = Some(connection_name.to_owned())
    }

    pub fn connection_name(&self) -> Option<String> {
        self.connection_name.clone()
    }

    pub fn id(&self) -> i64 {
        self.id
    }
}

pub struct Client<'a>(&'a mut Conn);

impl Client<'_> {
    pub fn new(conn: &mut Conn) -> Client {
        Client(conn)
    }
}

#[cfg_attr(test, automock)]
pub trait Connection {
    fn write_bulk(&mut self, msg: &[u8]);

    fn write_array(&mut self, count: usize);

    fn write_string(&mut self, msg: &str);

    fn write_integer(&mut self, x: i64);

    fn write_error(&mut self, err: ClientError);

    fn write_null(&mut self);

    fn context(&mut self) -> &mut Option<Box<dyn Any>>;
}

impl Connection for Client<'_> {
    fn write_bulk(&mut self, msg: &[u8]) {
        self.0.write_bulk(msg)
    }

    fn write_array(&mut self, count: usize) {
        self.0.write_array(count)
    }

    fn write_string(&mut self, msg: &str) {
        self.0.write_string(msg)
    }

    fn write_integer(&mut self, x: i64) {
        self.0.write_integer(x)
    }

    fn write_error(&mut self, err: ClientError) {
        self.0.write_error(format!("{}", err).as_str())
    }

    fn write_null(&mut self) {
        self.0.write_null()
    }

    fn context(&mut self) -> &mut Option<Box<dyn Any>> {
        &mut self.0.context
    }
}
