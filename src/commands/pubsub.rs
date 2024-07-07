use anyhow::Result;

use crate::{
    connection::{ClientError, Connection},
    subscription::MessageBus,
};

#[tracing::instrument(skip_all)]
pub fn subscribe(
    conn: &mut dyn Connection,
    message_bus: &mut dyn MessageBus,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let channel = String::from_utf8_lossy(&args[1]);
    message_bus.register_client(conn);
    message_bus.subscribe(conn, &channel);

    Ok(())
}
