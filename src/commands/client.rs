use redcon::Conn;
use rocksdb::DB;

use crate::connection::ConnectionContext;

#[tracing::instrument(skip_all)]
pub fn client(conn: &mut Conn, _db: &DB, args: &Vec<Vec<u8>>) {
    if args.len() < 2 {
        conn.write_error("ERR wrong number of arguments for command");
        return;
    }

    let subcommand = String::from_utf8_lossy(&args[1]).to_uppercase();
    match subcommand.as_str() {
        "SETINFO" => match &mut conn.context {
            Some(ctx) => {
                if args.len() < 4 {
                    conn.write_error("ERR wrong number of arguments for command");
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
                    _ => conn.write_error("ERR unknown attribute"),
                };
            }
            None => (),
        },
        _ => conn.write_error("ERR unknown command"),
    };

    conn.write_string("OK")
}
