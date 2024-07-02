use std::any::Any;

use redcon::Conn;

#[cfg(test)]
use mockall::automock;

pub struct ConnectionContext {
    lib_name: String,
    lib_version: String,
}

impl ConnectionContext {
    pub fn new() -> Self {
        ConnectionContext {
            lib_name: "".to_string(),
            lib_version: "".to_string(),
        }
    }

    pub fn set_lib_name(&mut self, lib_name: &str) {
        self.lib_name = lib_name.to_owned()
    }

    pub fn set_lib_version(&mut self, lib_version: &str) {
        self.lib_version = lib_version.to_owned()
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

    fn write_string(&mut self, msg: &str);

    fn write_integer(&mut self, x: i64);

    fn write_error(&mut self, msg: &str);

    fn write_null(&mut self);

    fn context(&mut self) -> &mut Option<Box<dyn Any>>;
}

impl Connection for Client<'_> {
    fn write_bulk(&mut self, msg: &[u8]) {
        self.0.write_bulk(msg)
    }

    fn write_string(&mut self, msg: &str) {
        self.0.write_string(msg)
    }

    fn write_integer(&mut self, x: i64) {
        self.0.write_integer(x)
    }

    fn write_error(&mut self, msg: &str) {
        self.0.write_error(msg)
    }

    fn write_null(&mut self) {
        self.0.write_null()
    }

    fn context(&mut self) -> &mut Option<Box<dyn Any>> {
        &mut self.0.context
    }
}
