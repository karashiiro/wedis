use crate::connection::{ClientError, Connection, ConnectionContext};

#[tracing::instrument(skip_all)]
pub fn client(conn: &mut dyn Connection, args: &Vec<Vec<u8>>) {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return;
    }

    let subcommand = String::from_utf8_lossy(&args[1]).to_uppercase();
    match subcommand.as_str() {
        "SETINFO" => match conn.context() {
            Some(ctx) => {
                if args.len() != 4 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let ctx = ctx
                    .downcast_mut::<ConnectionContext>()
                    .expect("context should be a ConnectionContext");

                let attribute_key = String::from_utf8_lossy(&args[2]).to_uppercase();
                let attribute_value = String::from_utf8_lossy(&args[3]);
                match attribute_key.as_str() {
                    "LIB-NAME" => {
                        ctx.set_lib_name(&attribute_value);
                        conn.write_string("OK");
                    }
                    "LIB-VER" => {
                        ctx.set_lib_version(&attribute_value);
                        conn.write_string("OK");
                    }
                    _ => conn.write_error(ClientError::UnknownAttribute),
                };
            }
            None => conn.write_error(ClientError::NoContext),
        },
        "SETNAME" => match conn.context() {
            Some(ctx) => {
                if args.len() != 3 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let ctx = ctx
                    .downcast_mut::<ConnectionContext>()
                    .expect("context should be a ConnectionContext");

                let connection_name = String::from_utf8_lossy(&args[2]);
                ctx.set_connection_name(&connection_name);
                conn.write_string("OK");
            }
            None => conn.write_error(ClientError::NoContext),
        },
        "GETNAME" => match conn.context() {
            Some(ctx) => {
                if args.len() != 2 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let ctx = ctx
                    .downcast_mut::<ConnectionContext>()
                    .expect("context should be a ConnectionContext");

                match ctx.connection_name() {
                    Some(connection_name) => conn.write_bulk(connection_name.as_bytes()),
                    None => conn.write_null(),
                };
            }
            None => conn.write_error(ClientError::NoContext),
        },
        "ID" => match conn.context() {
            Some(ctx) => {
                if args.len() != 2 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let ctx = ctx
                    .downcast_mut::<ConnectionContext>()
                    .expect("context should be a ConnectionContext");

                let id = ctx.id();
                conn.write_integer(id);
            }
            None => conn.write_error(ClientError::NoContext),
        },
        _ => conn.write_error(ClientError::UnknownCommand),
    }
}

#[tracing::instrument(skip_all)]
pub fn echo(conn: &mut dyn Connection, args: &Vec<Vec<u8>>) {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return;
    }

    conn.write_bulk(&args[1])
}

#[tracing::instrument(skip_all)]
pub fn ping(conn: &mut dyn Connection, args: &Vec<Vec<u8>>) {
    if args.len() == 1 {
        conn.write_string("PONG")
    } else if args.len() == 2 {
        conn.write_bulk(&args[1])
    } else {
        conn.write_error(ClientError::ArgCount)
    }
}

#[tracing::instrument(skip_all)]
pub fn quit(conn: &mut dyn Connection) {
    conn.write_string("OK")
}
