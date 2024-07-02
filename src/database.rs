use rocksdb::{ErrorKind, TransactionDB};
use thiserror::Error;

const TYPE_KEY_PREFIX: &str = "t:";
const DATA_KEY_PREFIX: &str = "d:";

const TYPE_STRING: &str = "S";

fn prepend_key(key: &[u8], prefix: &[u8]) -> Vec<u8> {
    [prefix, key].concat()
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("rocksdb error")]
    RocksDB(#[from] rocksdb::Error),
    #[error("unexpected value type (expected {expected:?})")]
    WrongType { expected: String },
}

pub struct Database {
    db: TransactionDB,
}

pub trait DatabaseOperations {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn delete(&self, key: &[u8]) -> Result<(), DatabaseError>;
}

impl Database {
    pub fn new(db: TransactionDB) -> Self {
        Self { db }
    }
}

impl DatabaseOperations for Database {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        Ok(self.db.get(key)?)
    }

    fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_key = prepend_key(key, TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key, DATA_KEY_PREFIX.as_bytes());

        let results =
            self.db
                .multi_get([type_key, data_key])
                .into_iter()
                .fold(Ok(vec![]), |agg, next| {
                    agg.and_then(|mut v1| {
                        next.and_then(|v2| {
                            v1.push(v2);
                            Ok(v1)
                        })
                    })
                });

        match results {
            Ok(v) => {
                let type_value = v[0].as_ref();
                match type_value {
                    Some(tv) => {
                        let data_value = v[1].clone();
                        if !tv.eq_ignore_ascii_case(TYPE_STRING.as_bytes()) {
                            Err(DatabaseError::WrongType {
                                expected: TYPE_STRING.to_string(),
                            })
                        } else {
                            Ok(data_value)
                        }
                    }
                    None => Ok(None),
                }
            }
            Err(err) => Err(DatabaseError::RocksDB(err)),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        Ok(self.db.put(key, value)?)
    }

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let type_key = prepend_key(key, TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key, DATA_KEY_PREFIX.as_bytes());

        let txn = self.db.transaction();
        txn.put(type_key, TYPE_STRING.as_bytes())?;
        txn.put(data_key, value)?;

        Ok(txn.commit()?)
    }

    fn delete(&self, key: &[u8]) -> Result<(), DatabaseError> {
        match self.db.delete(key) {
            Ok(_) => Ok(()),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(()),
                _ => Err(err.into()),
            },
        }
    }
}
