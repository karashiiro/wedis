use anyhow::Result;
use rocksdb::{ErrorKind, DB};

pub struct Database {
    db: DB,
}

pub trait DatabaseOperations {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;
}

impl Database {
    pub fn new(db: DB) -> Self {
        Self { db }
    }
}

impl DatabaseOperations for Database {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db.put(key, value)?)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        match self.db.delete(key) {
            Ok(_) => Ok(()),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(()),
                _ => Err(err.into()),
            },
        }
    }
}
