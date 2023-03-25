use crate::{memtable::MemTable, wal::wal::WAL};
use std::path::Path;

struct Database {
    memtable: MemTable,
    wal: WAL,
}

impl Database {
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> Result<(), std::io::Error> {
        self.memtable.set(key, value, timestamp);
        self.wal.set(key, value, timestamp)
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> Result<(), std::io::Error> {
        self.memtable.delete(key, timestamp);
        self.wal.delete(key, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Database {
        let path = Path::new("data");
        let wal = WAL::new(path).unwrap();
        let memtable = MemTable::new();
        Database { memtable, wal }
    }
    // create WAL, create Memtable
    // create database
    // create some data for input
    // after certain size, stop input and flush to sstable

    #[test]
    fn create_db() {
        let mut db = setup_db();
        let key = vec![0];
        let value = vec![1];
        let timestamp = 12;
        db.set(key.as_slice(), value.as_slice(), timestamp).ok();
    }
}
