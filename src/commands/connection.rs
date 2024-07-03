use crate::connection::{ClientError, Connection, ConnectionContext};

#[tracing::instrument(skip_all)]
pub fn client(conn: &mut dyn Connection, args: &Vec<Vec<u8>>) {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return;
    }

    let subcommand = String::from_utf8_lossy(&args[1]).to_uppercase();
    match subcommand.as_str() {
        "SETINFO" => match &mut conn.context() {
            Some(ctx) => {
                if args.len() < 4 {
                    conn.write_error(ClientError::ArgCount);
                    return;
                }

                let ctx = ctx
                    .downcast_mut::<ConnectionContext>()
                    .expect("context should be a ConnectionContext");

                let attribute_key = String::from_utf8_lossy(&args[2]).to_uppercase();
                let attribute_value = String::from_utf8_lossy(&args[3]);
                match attribute_key.as_str() {
                    "LIB-NAME" => ctx.set_lib_name(&attribute_value),
                    "LIB-VER" => ctx.set_lib_version(&attribute_value),
                    _ => conn.write_error(ClientError::UnknownAttribute),
                };
            }
            None => (),
        },
        _ => conn.write_error(ClientError::UnknownCommand),
    };

    conn.write_string("OK")
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
