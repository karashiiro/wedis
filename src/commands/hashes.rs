use anyhow::Result;
use itertools::Itertools;
use tracing::debug;

use crate::{
    connection::{ClientError, Connection},
    database::{DatabaseError, DatabaseOperations},
};

#[tracing::instrument(skip_all)]
pub fn hset(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    // Must have at least 4 args to declare "HSET key field value", and
    // increments of 2 more for additional field/value pairs
    if args.len() < 4 || args.len() % 2 != 0 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let key_value_pairs: Vec<(Vec<u8>, Vec<u8>)> = args[2..]
        .into_iter()
        .map(|x| x.clone())
        .tuples::<(_, _)>()
        .collect();
    match db.put_hash_fields(key, key_value_pairs) {
        Ok(n_fields) => {
            conn.write_integer(n_fields);
            Ok(())
        }
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[tracing::instrument(skip_all)]
pub fn hget(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    match db.get_hash_field(&args[1], &args[2]) {
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

#[tracing::instrument(skip_all)]
pub fn hstrlen(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    match db.get_hash_field(&args[1], &args[2]) {
        Ok(value) => {
            let val = value.unwrap_or_default();
            debug!("Hash field has length {}", val.len());
            Ok(conn.write_integer(val.len().try_into().unwrap()))
        }
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
    fn test_hset() {
        let key = "key";
        let field = "field";
        let value = "value";
        let fields: Vec<(Vec<u8>, Vec<u8>)> =
            vec![(field.as_bytes().to_vec(), value.as_bytes().to_vec())];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_put_hash_fields()
            .with(eq(key.as_bytes()), eq(fields))
            .times(1)
            .returning(|_, _| Ok(1));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["HSET".into(), key.into(), field.into(), value.into()];
        let _ = hset(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_hset_multi() {
        let key = "key";
        let field1 = "field1";
        let value1 = "value1";
        let field2 = "field2";
        let value2 = "value2";
        let fields: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (field1.as_bytes().to_vec(), value1.as_bytes().to_vec()),
            (field2.as_bytes().to_vec(), value2.as_bytes().to_vec()),
        ];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_put_hash_fields()
            .with(eq(key.as_bytes()), eq(fields))
            .times(1)
            .returning(|_, _| Ok(2));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(2))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "HSET".into(),
            key.into(),
            field1.into(),
            value1.into(),
            field2.into(),
            value2.into(),
        ];
        let _ = hset(&mut mock_conn, &mock_db, &args).unwrap();
    }
}
