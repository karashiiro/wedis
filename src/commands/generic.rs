use anyhow::Result;

use crate::{
    connection::{ClientError, Connection},
    database::DatabaseOperations,
};

#[tracing::instrument(skip_all)]
pub fn del(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let mut n_deleted = 0;
    for arg in args[1..].iter() {
        n_deleted += db.delete(&arg)?;
    }

    conn.write_integer(n_deleted);
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{connection::MockConnection, database::MockDatabaseOperations};
    use mockall::predicate::*;

    use super::*;

    #[test]
    fn test_del() {
        let key = "key";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_delete()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(1));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["DEL".into(), key.into()];
        let _ = del(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_del_multi() {
        let key1 = "key1";
        let key2 = "key2";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_delete()
            .with(eq(key1.as_bytes()))
            .times(1)
            .returning(|_| Ok(1));
        mock_db
            .expect_delete()
            .with(eq(key2.as_bytes()))
            .times(1)
            .returning(|_| Ok(1));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(2))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["DEL".into(), key1.into(), key2.into()];
        let _ = del(&mut mock_conn, &mock_db, &args).unwrap();
    }
}
