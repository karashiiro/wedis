use std::collections::HashMap;

use rocksdb::{ErrorKind, TransactionDB};
use thiserror::Error;

#[cfg(test)]
use mockall::automock;

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
    #[error("integer parse error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("unexpected value type (expected {expected:?})")]
    WrongType { expected: String },
}

pub struct Database {
    db: TransactionDB,
}

#[cfg_attr(test, automock)]
pub trait DatabaseOperations {
    fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_hash_field(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn put_hash_fields(
        &self,
        key: &[u8],
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<i64, DatabaseError>;

    fn increment_by(&self, key: &[u8], amount: i64) -> Result<i64, DatabaseError>;

    fn delete(&self, key: &[u8]) -> Result<i64, DatabaseError>;
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

    fn delete_typed_value<K: AsRef<[u8]>>(&self, key: K) -> Result<(), DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        let txn = self.db.transaction();
        txn.delete(type_key)?;
        txn.delete(data_key)?;

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

    fn put_hash_fields(
        &self,
        key: &[u8],
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<i64, DatabaseError> {
        // TODO: Update existing hash atomically
        let existing = self.get_typed_value(key, TYPE_HASH)?;

        let mut dict = match existing {
            Some(data) => {
                let hash = String::from_utf8_lossy(&data);
                let dict: HashMap<String, String> = serde_json::from_str(&hash)?;
                dict
            }
            None => HashMap::new(),
        };

        let mut n_fields = 0;
        for (field, value) in fields {
            // TODO: Avoid relying on encoding values as UTF-8 strings
            let field = String::from_utf8_lossy(&field).into_owned();
            let value = String::from_utf8_lossy(&value).into_owned();
            dict.insert(field, value);
            n_fields += 1;
        }

        let value = serde_json::to_string(&dict)?;
        self.put_typed_value(key, value, TYPE_HASH)?;

        Ok(n_fields)
    }

    fn increment_by(&self, key: &[u8], amount: i64) -> Result<i64, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        let txn = self.db.transaction();
        let current_value = txn
            .get_for_update(data_key.clone(), true)?
            .unwrap_or_else(|| "0".as_bytes().to_vec());

        // This needs to be a valid UTF-8 string in order to parse it
        let current_value = String::from_utf8_lossy(&current_value).into_owned();
        let current_value = current_value.parse::<i64>()?;
        let next_value = current_value + amount;

        txn.put(type_key, TYPE_STRING.as_bytes())?;
        txn.put(data_key, next_value.to_string().as_bytes())?;

        txn.commit()?;

        Ok(next_value)
    }

    fn delete(&self, key: &[u8]) -> Result<i64, DatabaseError> {
        match self.delete_typed_value(key) {
            Ok(()) => Ok(1),
            Err(DatabaseError::RocksDB(err)) => match err.kind() {
                ErrorKind::NotFound => Ok(0),
                _ => Err(err.into()),
            },
            Err(err) => Err(err),
        }
    }
}
