use std::collections::HashMap;

use rocksdb::{ErrorKind, TransactionDB};
use thiserror::Error;

const TYPE_KEY_PREFIX: &str = "t:";
const DATA_KEY_PREFIX: &str = "d:";

const TYPE_STRING: &str = "S";
const TYPE_HASH: &str = "H";

fn prepend_key(key: &[u8], prefix: &[u8]) -> Vec<u8> {
    [prefix, key].concat()
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("rocksdb error")]
    RocksDB(#[from] rocksdb::Error),
    #[error("serialization error")]
    Serde(#[from] serde_json::Error),
    #[error("unexpected value type (expected {expected:?})")]
    WrongType { expected: String },
}

pub struct Database {
    db: TransactionDB,
}

pub trait DatabaseOperations {
    fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_hash_field(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn put_hash_field(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn delete(&self, key: &[u8]) -> Result<(), DatabaseError>;
}

impl Database {
    pub fn new(db: TransactionDB) -> Self {
        Self { db }
    }

    fn get_pair<K: AsRef<[u8]>>(
        &self,
        key1: K,
        key2: K,
    ) -> Result<Vec<Option<Vec<u8>>>, rocksdb::Error> {
        self.db
            .multi_get([key1, key2])
            .into_iter()
            .fold(Ok(vec![]), |agg, next| {
                agg.and_then(|mut results| {
                    next.and_then(|value| {
                        results.push(value);
                        Ok(results)
                    })
                })
            })
    }

    fn get_typed_value<K: AsRef<[u8]>>(
        &self,
        key: K,
        type_id: &str,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        let results = self.get_pair(type_key, data_key);
        match results {
            Ok(v) => {
                let type_value = v[0].as_ref();
                match type_value {
                    Some(tv) => {
                        let data_value = v[1].clone();
                        if !tv.eq_ignore_ascii_case(type_id.as_bytes()) {
                            Err(DatabaseError::WrongType {
                                expected: type_id.to_string(),
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

    fn put_typed_value<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        type_id: &str,
    ) -> Result<(), DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        let txn = self.db.transaction();
        txn.put(type_key, type_id.as_bytes())?;
        txn.put(data_key, value)?;

        Ok(txn.commit()?)
    }
}

impl DatabaseOperations for Database {
    fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.get_typed_value(key, TYPE_STRING)
    }

    fn get_hash_field(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let hash = self.get_typed_value(key, TYPE_HASH)?;
        if let None = hash {
            return Ok(None);
        }

        let hash = hash.unwrap();
        let hash = String::from_utf8_lossy(&hash);
        let dict: HashMap<String, String> = serde_json::from_str(&hash)?;

        let subkey = String::from_utf8_lossy(field).into_owned();
        let value = dict.get(&subkey);
        if let None = value {
            return Ok(None);
        }

        let value = value.unwrap();
        Ok(Some(value.as_bytes().to_vec()))
    }

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.put_typed_value(key, value, TYPE_STRING)
    }

    fn put_hash_field(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // TODO: Update existing hash instead of overwriting it
        let _ = self.get_typed_value(key, TYPE_HASH)?;

        // TODO: Avoid relying on encoding values as UTF-8 strings
        let field = String::from_utf8_lossy(field).into_owned();
        let value = String::from_utf8_lossy(value).into_owned();

        let mut dict = HashMap::new();
        dict.insert(field, value);

        let value = serde_json::to_string(&dict)?;
        self.db.put(&key, value.as_bytes())?;

        self.put_typed_value(key, value, TYPE_HASH)
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
