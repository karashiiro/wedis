use std::time::Duration;

use anyhow::Result;
use tracing::debug;

use crate::{
    connection::{ClientError, Connection},
    database::DatabaseOperations,
    time::unix_timestamp,
};

#[tracing::instrument(skip_all)]
pub fn persist(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    match db.delete_expiry(&key) {
        Ok(_) => {
            conn.write_integer(1);
            Ok(())
        }
        Err(err) => {
            conn.write_integer(0);
            Err(err.into())
        }
    }
}

#[tracing::instrument(skip_all)]
pub fn expireat(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let ts = String::from_utf8_lossy(&args[2]).parse::<u64>()?;
    let expires_at = Duration::from_secs(ts);
    let expires_in = expires_at.saturating_sub(unix_timestamp()?);

    match db.put_expiry(&key, expires_in) {
        Ok(_) => {
            conn.write_integer(1);
            Ok(())
        }
        Err(err) => {
            conn.write_integer(0);
            Err(err.into())
        }
    }
}

#[tracing::instrument(skip_all)]
pub fn pexpireat(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let ts = String::from_utf8_lossy(&args[2]).parse::<u64>()?;
    let expires_at = Duration::from_millis(ts);
    let expires_in = expires_at.saturating_sub(unix_timestamp()?);

    match db.put_expiry(&key, expires_in) {
        Ok(_) => {
            conn.write_integer(1);
            Ok(())
        }
        Err(err) => {
            conn.write_integer(0);
            Err(err.into())
        }
    }
}

#[tracing::instrument(skip_all)]
pub fn expire(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let secs = String::from_utf8_lossy(&args[2]).parse::<u64>()?;
    let expires_in = Duration::from_secs(secs);

    let mut update_expiry = || match db.put_expiry(&key, expires_in) {
        Ok(_) => Ok(conn.write_integer(1)),
        Err(err) => {
            conn.write_integer(0);
            Err(err.into())
        }
    };

    let mut options: Vec<String> = vec![];
    for arg in args[3..].iter() {
        options.push(String::from_utf8_lossy(&arg).into_owned().to_uppercase());
    }

    let nx = options.contains(&"NX".to_string());
    let xx = options.contains(&"XX".to_string());
    let gt = options.contains(&"GT".to_string());
    let lt = options.contains(&"LT".to_string());

    if nx && (xx || gt || lt) {
        return Ok(conn.write_error(ClientError::ExpireNxOptions));
    }

    if nx {
        match db.get_expiry(&key)? {
            Some(_) => Ok(conn.write_integer(0)),
            None => update_expiry(),
        }
    } else if gt {
        match db.get_expiry(&key)? {
            Some(ttl) if { expires_in < ttl } => Ok(conn.write_integer(0)),
            None if { xx } => Ok(conn.write_integer(0)),
            _ => update_expiry(),
        }
    } else if lt {
        match db.get_expiry(&key)? {
            Some(ttl) if { expires_in > ttl } => Ok(conn.write_integer(0)),
            None if { xx } => Ok(conn.write_integer(0)),
            _ => update_expiry(),
        }
    } else if xx {
        match db.get_expiry(&key)? {
            Some(_) => update_expiry(),
            _ => Ok(conn.write_integer(0)),
        }
    } else {
        // No options
        update_expiry()
    }
}

#[tracing::instrument(skip_all)]
pub fn pexpire(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let ms = String::from_utf8_lossy(&args[2]).parse::<u64>()?;
    let expires_in = Duration::from_millis(ms);

    match db.put_expiry(&key, expires_in) {
        Ok(_) => {
            conn.write_integer(1);
            Ok(())
        }
        Err(err) => {
            conn.write_integer(0);
            Err(err.into())
        }
    }
}

#[tracing::instrument(skip_all)]
pub fn ttl(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];

    let ttl = db.get_expiry(key)?;
    if let None = ttl {
        return match db.exists(key)? {
            0 => Ok(conn.write_integer(-2)),
            _ => Ok(conn.write_integer(-1)),
        };
    }

    let ttl: i64 = ttl.unwrap().as_secs().try_into()?;
    conn.write_integer(ttl);

    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn pttl(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];

    let ttl = db.get_expiry(key)?;
    if let None = ttl {
        return match db.exists(key)? {
            0 => Ok(conn.write_integer(-2)),
            _ => Ok(conn.write_integer(-1)),
        };
    }

    let ttl: i64 = ttl.unwrap().as_millis().try_into()?;
    conn.write_integer(ttl);

    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn expiretime(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];

    let ttl = db.get_expiry(key)?;
    if let None = ttl {
        return match db.exists(key)? {
            0 => Ok(conn.write_integer(-2)),
            _ => Ok(conn.write_integer(-1)),
        };
    }

    let ttl: i64 = ttl
        .unwrap()
        .saturating_add(unix_timestamp()?)
        .as_secs()
        .try_into()?;
    conn.write_integer(ttl);

    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn pexpiretime(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() != 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];

    let ttl = db.get_expiry(key)?;
    if let None = ttl {
        return match db.exists(key)? {
            0 => Ok(conn.write_integer(-2)),
            _ => Ok(conn.write_integer(-1)),
        };
    }

    let ttl: i64 = ttl
        .unwrap()
        .saturating_add(unix_timestamp()?)
        .as_millis()
        .try_into()?;
    conn.write_integer(ttl);

    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn unlink(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    del(conn, db, args)
}

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

    debug!("Deleted {} values", n_deleted);

    conn.write_integer(n_deleted);
    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn exists(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 2 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let mut n_exists = 0;
    for arg in args[1..].iter() {
        n_exists += db.exists(&arg)?;
    }

    debug!("{} queried values exist", n_exists);

    conn.write_integer(n_exists);
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
