use std::{collections::HashMap, time::Duration};

use itertools::Itertools;
use rocksdb::{Transaction, TransactionDB};
use thiserror::Error;

#[cfg(test)]
use mockall::automock;

use crate::time::{parse_timestamp, serialize_duration_as_timestamp, unix_timestamp, TimeError};

const TTL_KEY_PREFIX: &str = "T:";
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
    #[error("float parse error")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("time error")]
    InvalidTime(#[from] TimeError),
    #[error("unexpected value type (expected {expected:?})")]
    WrongType { expected: String },
}

pub struct Database {
    connect_count: i64,
    db: TransactionDB,
}

#[cfg_attr(test, automock)]
pub trait DatabaseOperations {
    fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_hash_field(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_expiry(&self, key: &[u8]) -> Result<Option<Duration>, DatabaseError>;

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn put_hash_fields(
        &self,
        key: &[u8],
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<i64, DatabaseError>;

    fn put_expiry(&self, key: &[u8], expires_in: Duration) -> Result<(), DatabaseError>;

    fn exists(&self, key: &[u8]) -> Result<i64, DatabaseError>;

    fn increment_by(&self, key: &[u8], amount: i64) -> Result<i64, DatabaseError>;

    fn increment_by_float(&self, key: &[u8], amount: f64) -> Result<f64, DatabaseError>;

    fn delete(&self, key: &[u8]) -> Result<i64, DatabaseError>;

    fn delete_expiry(&self, key: &[u8]) -> Result<i64, DatabaseError>;
}

trait RString = AsRef<[u8]>;

impl Database {
    pub fn new(db: TransactionDB) -> Self {
        Self {
            db,
            connect_count: 0,
        }
    }

    pub fn acquire_connection(&mut self) -> i64 {
        let current = self.connect_count;
        self.connect_count += 1;
        current
    }

    fn put_expiry<K: RString>(&self, key: K, expires_in: Duration) -> Result<(), DatabaseError> {
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());
        let ttl_ms = serialize_duration_as_timestamp(expires_in)?;

        // Begin a transaction on the data key to ensure we don't set
        // a TTL while the value is being replaced.
        let txn = self.db.transaction();
        txn.get_for_update(data_key, true)?;

        // Set the TTL
        txn.put(ttl_key, ttl_ms)?;

        Ok(txn.commit()?)
    }

    fn get_expiry<K: RString>(&self, key: K) -> Result<Option<Duration>, DatabaseError> {
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());
        let ttl = self.db.get(ttl_key)?;

        match ttl {
            Some(ttl) => Ok(Some(
                parse_timestamp(&ttl)?.saturating_sub(unix_timestamp()?),
            )),
            None => Ok(None),
        }
    }

    fn delete_expiry<K: RString>(&self, key: K) -> Result<i64, DatabaseError> {
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());

        // Begin a transaction on the data key to ensure we don't set
        // a TTL while the value is being replaced.
        let txn = self.db.transaction();
        txn.get_for_update(data_key, true)?;

        let existing_ttl = txn.get_for_update(ttl_key.clone(), true)?;
        if let None = existing_ttl {
            return Ok(0);
        }

        // Delete the TTL
        txn.delete(ttl_key)?;
        txn.commit()?;

        Ok(1)
    }

    fn get_triple<K: RString>(
        &self,
        key1: K,
        key2: K,
        key3: K,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>), rocksdb::Error> {
        let result =
            self.db
                .multi_get([key1, key2, key3])
                .into_iter()
                .fold(Ok(vec![]), |agg, next| {
                    agg.and_then(|mut results| {
                        next.and_then(|value| {
                            results.push(value);
                            Ok(results)
                        })
                    })
                })?;

        // This will always succeed if we reach this point
        Ok(result.into_iter().next_tuple::<(_, _, _)>().unwrap())
    }

    fn get_triple_for_update<K: RString>(
        &self,
        txn: &Transaction<TransactionDB>,
        key1: K,
        key2: K,
        key3: K,
        exclusive: bool,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>), rocksdb::Error> {
        let val1 = txn.get_for_update(key1, exclusive)?;
        let val2 = txn.get_for_update(key2, exclusive)?;
        let val3 = txn.get_for_update(key3, exclusive)?;
        Ok((val1, val2, val3))
    }

    fn get_typed_value<K: RString>(
        &self,
        key: K,
        type_id: &str,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());

        let (type_value, data_value, ttl_value) = self.get_triple(type_key, data_key, ttl_key)?;
        if let Some(ttl) = ttl_value {
            let ttl = parse_timestamp(&ttl)?.saturating_sub(unix_timestamp()?);
            if ttl == Duration::ZERO {
                return Ok(None);
            }
        }

        Self::validate_typed_value(&type_value, type_id).and_then(|_| Ok(data_value))
    }

    fn get_typed_value_for_update<K: RString>(
        &self,
        txn: &Transaction<TransactionDB>,
        key: K,
        type_id: &str,
        exclusive: bool,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());

        let (type_value, data_value, ttl_value) =
            self.get_triple_for_update(txn, type_key, data_key, ttl_key, exclusive)?;
        if let Some(ttl) = ttl_value {
            let ttl = parse_timestamp(&ttl)?.saturating_sub(unix_timestamp()?);
            if ttl == Duration::ZERO {
                return Ok(None);
            }
        }

        Self::validate_typed_value(&type_value, type_id).and_then(|_| Ok(data_value))
    }

    fn validate_typed_value(
        type_value: &Option<Vec<u8>>,
        expected_type_id: &str,
    ) -> Result<(), DatabaseError> {
        type_value.as_ref().map_or_else(
            || Ok(()),
            |tv| {
                if !tv.eq_ignore_ascii_case(expected_type_id.as_bytes()) {
                    Err(DatabaseError::WrongType {
                        expected: expected_type_id.to_string(),
                    })
                } else {
                    Ok(())
                }
            },
        )
    }

    fn put_typed_value<K: RString, V: RString>(
        &self,
        key: K,
        value: V,
        type_id: &str,
    ) -> Result<(), DatabaseError> {
        let txn = self.db.transaction();
        self.put_typed_value_txn(&txn, key, value, type_id)?;
        Ok(txn.commit()?)
    }

    fn put_typed_value_txn<K: RString, V: RString>(
        &self,
        txn: &Transaction<TransactionDB>,
        key: K,
        value: V,
        type_id: &str,
    ) -> Result<(), DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());

        txn.put(type_key, type_id.as_bytes())?;
        txn.put(data_key, value)?;
        txn.delete(ttl_key)?;

        Ok(())
    }

    fn delete_typed_value<K: RString>(&self, key: K) -> Result<(), DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let data_key = prepend_key(key.as_ref(), DATA_KEY_PREFIX.as_bytes());
        let ttl_key = prepend_key(key.as_ref(), TTL_KEY_PREFIX.as_bytes());

        let txn = self.db.transaction();
        txn.delete(type_key)?;
        txn.delete(data_key)?;
        txn.delete(ttl_key)?;

        Ok(txn.commit()?)
    }

    fn exists<K: RString>(&self, key: K) -> Result<bool, DatabaseError> {
        let type_key = prepend_key(key.as_ref(), TYPE_KEY_PREFIX.as_bytes());
        let type_value = self.db.get(type_key)?;
        match type_value {
            Some(_) => Ok(true),
            None => Ok(false),
        }
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

    fn get_expiry(&self, key: &[u8]) -> Result<Option<Duration>, DatabaseError> {
        self.get_expiry(key)
    }

    fn put_string(&self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.put_typed_value(key, value, TYPE_STRING)
    }

    fn put_hash_fields(
        &self,
        key: &[u8],
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<i64, DatabaseError> {
        let txn = self.db.transaction();
        let existing = self.get_typed_value_for_update(&txn, key, TYPE_HASH, true)?;

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
        self.put_typed_value_txn(&txn, key, value, TYPE_HASH)?;

        txn.commit()?;

        Ok(n_fields)
    }

    fn put_expiry(&self, key: &[u8], expires_in: Duration) -> Result<(), DatabaseError> {
        self.put_expiry(key, expires_in)
    }

    fn exists(&self, key: &[u8]) -> Result<i64, DatabaseError> {
        match self.exists(key)? {
            true => Ok(1),
            false => Ok(0),
        }
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

        txn.commit()?;

        Ok(next_value)
    }

    fn increment_by_float(&self, key: &[u8], amount: f64) -> Result<f64, DatabaseError> {
        let txn = self.db.transaction();
        let current_value = self
            .get_typed_value_for_update(&txn, key, TYPE_STRING, true)?
            .unwrap_or_else(|| "0".as_bytes().to_vec());

        // This needs to be a valid UTF-8 string in order to parse it
        let current_value = String::from_utf8_lossy(&current_value).into_owned();
        let current_value = current_value.parse::<f64>()?;
        let next_value = current_value + amount;

        self.put_typed_value_txn(&txn, key, next_value.to_string().as_bytes(), TYPE_STRING)?;

        txn.commit()?;

        Ok(next_value)
    }

    fn delete(&self, key: &[u8]) -> Result<i64, DatabaseError> {
        if !self.exists(key)? {
            return Ok(0);
        }

        self.delete_typed_value(key).and_then(|_| Ok(1))
    }

    fn delete_expiry(&self, key: &[u8]) -> Result<i64, DatabaseError> {
        self.delete_expiry(key)
    }
}
