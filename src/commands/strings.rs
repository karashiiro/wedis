use anyhow::Result;

use crate::{
    connection::{ClientError, Connection},
    database::{DatabaseError, DatabaseOperations},
};

#[tracing::instrument(skip_all)]
pub fn set(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    db.put_string(&args[1], &args[2])?;

    conn.write_string("OK");
    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn get(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    match db.get_string(&args[1]) {
        Ok(value) => match value {
            Some(val) => Ok(conn.write_bulk(&val)),
            None => Ok(conn.write_null()),
        },
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod test {
    use crate::{connection::MockConnection, database::MockDatabaseOperations};
    use mockall::predicate::*;

    use super::*;

    #[test]
    fn test_get() {
        let key = "key";
        let value = "value";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_bulk()
            .with(eq(value.as_bytes()))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["GET".into(), key.into()];
        let _ = get(&mut mock_conn, &mock_db, &args).unwrap();
    }
}
