use std::collections::HashMap;

use rocksdb::{ErrorKind, Transaction, TransactionDB};
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

    fn get_pair_for_update<K: AsRef<[u8]>>(
        &self,
        txn: &Transaction<TransactionDB>,
        key1: K,
        key2: K,
        exclusive: bool,
    ) -> Result<Vec<Option<Vec<u8>>>, rocksdb::Error> {
        let val1 = txn.get_for_update(key1, exclusive)?;
        let val2 = txn.get_for_update(key2, exclusive)?;
        Ok(vec![val1, val2])
    }

    fn get_typed_value<K: AsRef<[u8]>>(
        &self,
        key: K,
        type_id: &str,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        let results = self.get_pair(type_key, data_key)?;
        Self::validate_typed_value(results, type_id)
    }

    fn get_typed_value_for_update<K: AsRef<[u8]>>(
        &self,
        txn: &Transaction<TransactionDB>,
        key: K,
        type_id: &str,
        exclusive: bool,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        let results = self.get_pair_for_update(txn, type_key, data_key, exclusive)?;
        Self::validate_typed_value(results, type_id)
    }

    fn validate_typed_value(
        pair: Vec<Option<Vec<u8>>>,
        type_id: &str,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_value = pair[0].as_ref();
        type_value.map_or_else(
            || Ok(None),
            |tv| {
                let data_value = pair[1].clone();
                if !tv.eq_ignore_ascii_case(type_id.as_bytes()) {
                    Err(DatabaseError::WrongType {
                        expected: type_id.to_string(),
                    })
                } else {
                    Ok(data_value)
                }
            },
        )
    }

    fn put_typed_value<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        type_id: &str,
    ) -> Result<(), DatabaseError> {
        let txn = self.db.transaction();
        self.put_typed_value_txn(&txn, key, value, type_id)?;
        Ok(txn.commit()?)
    }

    fn put_typed_value_txn<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        txn: &Transaction<TransactionDB>,
        key: K,
        value: V,
        type_id: &str,
    ) -> Result<(), DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());

        txn.put(type_key, type_id.as_bytes())?;
        txn.put(data_key, value)?;

        Ok(())
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
        let txn = self.db.transaction();
        let current_value = self
            .get_typed_value_for_update(&txn, key, TYPE_STRING, true)?
            .unwrap_or_else(|| "0".as_bytes().to_vec());

        // This needs to be a valid UTF-8 string in order to parse it
        let current_value = String::from_utf8_lossy(&current_value).into_owned();
        let current_value = current_value.parse::<i64>()?;
        let next_value = current_value + amount;

        self.put_typed_value_txn(&txn, key, next_value.to_string().as_bytes(), TYPE_STRING)?;

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
