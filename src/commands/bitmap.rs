use anyhow::Result;
use tracing::debug;

use crate::{
    connection::{ClientError, Connection},
    database::{DatabaseError, DatabaseOperations},
    indexing::adjust_indices,
};

#[tracing::instrument(skip_all)]
pub fn bitcount(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 2 || args.len() > 5 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];

    match db.get_string(key) {
        Ok(value) => {
            let val = value.unwrap_or_default();
            debug!("Retrieved value {:?}", String::from_utf8_lossy(&val));

            // TODO: Handle BIT option
            if args.len() >= 4 {
                let start = String::from_utf8_lossy(&args[2]).parse::<i64>()?;
                let end = String::from_utf8_lossy(&args[3]).parse::<i64>()?;
                let (start, end) = adjust_indices(val.len() - 1, start, end);
                let bits: i64 = popcnt::count_ones(&val[start..=end]).try_into().unwrap();
                Ok(conn.write_integer(bits))
            } else {
                let bits: i64 = popcnt::count_ones(&val).try_into().unwrap();
                Ok(conn.write_integer(bits))
            }
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
    fn test_bitcount() {
        let key = "key";
        let value = "foobar";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(26))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["BITCOUNT".into(), key.into()];
        let _ = bitcount(&mut mock_conn, &mock_db, &args).unwrap();
    }
}
