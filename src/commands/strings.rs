use std::time::Duration;

use anyhow::Result;
use tracing::debug;

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
pub fn setex(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 4 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let secs = String::from_utf8_lossy(&args[2]).parse::<u64>()?;
    let expires_in = Duration::from_secs(secs);

    db.put_string(key, &args[3])?;
    db.put_expiry(key, expires_in)?;

    conn.write_string("OK");
    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn strlen(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    match db.get_string(&args[1]) {
        Ok(value) => value
            .map_or(Ok(0), |v| Ok(v.len()))
            .and_then(|n| Ok(conn.write_integer(n.try_into().unwrap()))),
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

fn adjust_index(end_index: usize, x: i64) -> usize {
    let iend_index: i64 = end_index.try_into().unwrap();
    if x > iend_index {
        end_index
    } else if x >= 0 {
        x.try_into().unwrap()
    } else {
        // x < 0
        (iend_index + x + 1).try_into().unwrap()
    }
}

fn adjust_indices(end_index: usize, start: i64, end: i64) -> (usize, usize) {
    (adjust_index(end_index, start), adjust_index(end_index, end))
}

#[tracing::instrument(skip_all)]
pub fn getrange(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    substr(conn, db, args)
}

#[tracing::instrument(skip_all)]
pub fn substr(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 4 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let start = String::from_utf8_lossy(&args[2]).parse::<i64>()?;
    let end = String::from_utf8_lossy(&args[3]).parse::<i64>()?;

    match db.get_string(key) {
        Ok(value) => match value {
            Some(val) => {
                if val.len() == 0 {
                    return Ok(conn.write_bulk("".as_bytes()));
                }

                let (start, end) = adjust_indices(val.len() - 1, start, end);
                if start < end {
                    let result = &val[start..=end];
                    debug!("Returning value {}", String::from_utf8_lossy(&result));
                    Ok(conn.write_bulk(&result))
                } else {
                    Ok(conn.write_bulk("".as_bytes()))
                }
            }
            None => Ok(conn.write_bulk("".as_bytes())),
        },
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
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
            Some(val) => {
                debug!("Retrieved value {:?}", String::from_utf8_lossy(&val));
                Ok(conn.write_bulk(&val))
            }
            None => {
                debug!("Value does not exist");
                Ok(conn.write_null())
            }
        },
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[tracing::instrument(skip_all)]
pub fn incr(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    match db.increment_by(&args[1], 1) {
        Ok(value) => Ok(conn.write_integer(value)),
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[tracing::instrument(skip_all)]
pub fn incrby(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let amount = String::from_utf8_lossy(&args[2])
        .into_owned()
        .parse::<i64>()?;
    match db.increment_by(&args[1], amount) {
        Ok(value) => Ok(conn.write_integer(value)),
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[tracing::instrument(skip_all)]
pub fn decr(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    match db.increment_by(&args[1], -1) {
        Ok(value) => Ok(conn.write_integer(value)),
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[tracing::instrument(skip_all)]
pub fn decrby(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let amount = String::from_utf8_lossy(&args[2])
        .into_owned()
        .parse::<i64>()?;
    match db.increment_by(&args[1], -amount) {
        Ok(value) => Ok(conn.write_integer(value)),
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

    #[test]
    fn test_strlen() {
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
            .expect_write_integer()
            .with(eq(5))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["STRLEN".into(), key.into()];
        let _ = strlen(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_adjust_indices_negative() {
        let end_index = 4;
        let start = -3;
        let end = -1;

        let (start, end) = adjust_indices(end_index, start, end);
        assert_eq!(2, start);
        assert_eq!(4, end);
    }

    #[test]
    fn test_substr() {
        let key = "key";
        let value = "value";

        let start = 0;
        let end = 2;

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_bulk()
            .with(eq("val".as_bytes()))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "SUBSTR".into(),
            key.into(),
            start.to_string().into(),
            end.to_string().into(),
        ];
        let _ = substr(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_substr_negative() {
        let key = "key";
        let value = "value";

        let start = -3;
        let end = -1;

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_bulk()
            .with(eq("lue".as_bytes()))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "SUBSTR".into(),
            key.into(),
            start.to_string().into(),
            end.to_string().into(),
        ];
        let _ = substr(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_substr_out_of_range() {
        let key = "key";
        let value = "value";

        let start = 0;
        let end = 10;

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_bulk()
            .with(eq("value".as_bytes()))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "SUBSTR".into(),
            key.into(),
            start.to_string().into(),
            end.to_string().into(),
        ];
        let _ = substr(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_incr() {
        let key = "key";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_increment_by()
            .with(eq(key.as_bytes()), eq(1))
            .times(1)
            .returning(|_, _| Ok(2));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(2))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["INCR".into(), key.into()];
        let _ = incr(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_incrby() {
        let key = "key";
        let amount = 3;

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_increment_by()
            .with(eq(key.as_bytes()), eq(amount))
            .times(1)
            .returning(|_, _| Ok(5));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(5))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["INCRBY".into(), key.into(), amount.to_string().into()];
        let _ = incrby(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_decr() {
        let key = "key";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_increment_by()
            .with(eq(key.as_bytes()), eq(-1))
            .times(1)
            .returning(|_, _| Ok(0));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(0))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["DECR".into(), key.into()];
        let _ = decr(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_decrby() {
        let key = "key";
        let amount = 3;

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_increment_by()
            .with(eq(key.as_bytes()), eq(-amount))
            .times(1)
            .returning(|_, _| Ok(1));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["DECRBY".into(), key.into(), amount.to_string().into()];
        let _ = decrby(&mut mock_conn, &mock_db, &args).unwrap();
    }
}
