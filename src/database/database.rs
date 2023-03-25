use crate::{memtable::MemTable, wal::wal::WAL};
use std::path::Path;

struct Database {
    memtable: MemTable,
    wal: WAL,
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
        db.wal.set(key.as_slice(), value.as_slice(), timestamp).ok();
        db.memtable.set(key.as_slice(), value.as_slice(), timestamp);
    }
}
