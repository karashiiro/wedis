use crate::connection::{ClientError, Connection};

#[tracing::instrument(skip_all)]
pub fn client(conn: &mut dyn Connection, args: &Vec<Vec<u8>>) {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return;
    }

    let subcommand = String::from_utf8_lossy(&args[1]).to_uppercase();
    match subcommand.as_str() {
        "SETINFO" => match conn.context() {
            Some(_) => {
                if args.len() != 4 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let attribute_key = String::from_utf8_lossy(&args[2]).to_uppercase();
                let attribute_value = String::from_utf8_lossy(&args[3]);
                match attribute_key.as_str() {
                    "LIB-NAME" => {
                        conn.set_lib_name(&attribute_value);
                        conn.write_string("OK");
                    }
                    "LIB-VER" => {
                        conn.set_lib_version(&attribute_value);
                        conn.write_string("OK");
                    }
                    _ => conn.write_error(ClientError::UnknownAttribute),
                };
            }
            None => conn.write_error(ClientError::NoContext),
        },
        "SETNAME" => match conn.context() {
            Some(_) => {
                if args.len() != 3 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let connection_name = String::from_utf8_lossy(&args[2]);
                conn.set_connection_name(&connection_name);
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

                match ctx.connection_name() {
                    Some(connection_name) => conn.write_bulk(connection_name.as_bytes()),
                    None => conn.write_null(),
                };
            }
            None => conn.write_error(ClientError::NoContext),
        },
        "ID" => {
            if args.len() != 2 {
                conn.write_error(ClientError::ArgCount);
                return;
            }

            let id = conn.connection_id();
            conn.write_integer(id.try_into().unwrap());
        }
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
pub fn hello(conn: &mut dyn Connection, args: &Vec<Vec<u8>>) {
    if args.len() != 1 {
        conn.write_error(ClientError::ArgCount);
        return;
    }

    conn.write_array(14);
    conn.write_string("server");
    conn.write_string("redis");
    conn.write_string("version");
    conn.write_string("7.2.5");
    conn.write_string("proto");
    conn.write_integer(2);
    conn.write_string("id");

    let connection_id = conn.connection_id();
    conn.write_integer(connection_id.try_into().unwrap());
    conn.write_string("mode");
    conn.write_string("standalone");
    conn.write_string("role");
    conn.write_string("master");
    conn.write_string("modules");
    conn.write_array(0);
}

#[tracing::instrument(skip_all)]
pub fn quit(conn: &mut dyn Connection) {
    conn.write_string("OK")
}
