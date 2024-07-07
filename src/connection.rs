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
    #[error("bit offset is not an integer or out of range")]
    BitOffset,
    #[error("NX and XX, GT or LT options at the same time are not compatible")]
    ExpireNxOptions,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}

#[derive(Clone)]
pub struct ConnectionContext {
    lib_name: String,
    lib_version: String,
    connection_name: Option<String>,
}

impl ConnectionContext {
    pub fn new() -> Self {
        ConnectionContext {
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

    fn context(&self) -> Option<ConnectionContext>;

    fn connection_id(&self) -> u64;

    fn set_lib_name(&mut self, lib_name: &str);

    fn set_lib_version(&mut self, lib_version: &str);

    fn set_connection_name(&mut self, connection_name: &str);
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

    fn context(&self) -> Option<ConnectionContext> {
        self.0
            .context
            .as_ref()
            .and_then(|ctx| ctx.downcast_ref::<ConnectionContext>())
            .and_then(|ctx| Some(ctx.clone()))
    }

    fn connection_id(&self) -> u64 {
        self.0.id()
    }

    fn set_lib_name(&mut self, lib_name: &str) {
        let ctx = self
            .0
            .context
            .as_mut()
            .and_then(|ctx| ctx.downcast_mut::<ConnectionContext>());
        match ctx {
            Some(ctx) => ctx.set_lib_name(lib_name),
            None => (),
        }
    }

    fn set_lib_version(&mut self, lib_version: &str) {
        let ctx = self
            .0
            .context
            .as_mut()
            .and_then(|ctx| ctx.downcast_mut::<ConnectionContext>());
        match ctx {
            Some(ctx) => ctx.set_lib_version(lib_version),
            None => (),
        }
    }

    fn set_connection_name(&mut self, connection_name: &str) {
        let ctx = self
            .0
            .context
            .as_mut()
            .and_then(|ctx| ctx.downcast_mut::<ConnectionContext>());
        match ctx {
            Some(ctx) => ctx.set_connection_name(connection_name),
            None => (),
        }
    }
}
