use crate::{
    memtable::{memtable::MemTableEntry, MemTable},
    sstable::sstable::SSTable,
    wal::wal::WAL,
};
use std::io;

use std::path::Path;

struct Database {
    memtable: MemTable,
    wal: WAL,
}

impl Database {
    pub fn new(dir: &Path) -> io::Result<Database> {
        let (wal, memtable) = WAL::load_from_dir(dir)?; // if this fails, Err will be returned
        Ok(Database { wal, memtable })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> Result<(), std::io::Error> {
        self.memtable.set(key, value, timestamp);
        self.wal.set(key, value, timestamp)
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> Result<(), std::io::Error> {
        self.memtable.delete(key, timestamp);
        self.wal.delete(key, timestamp)
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        self.memtable.get(key)
        // if None -> search sstables for the value, only after everything has been searched its
        // not in there
    }
    fn flush(&self, dir: &Path) -> io::Result<()> {
        let mut sstable = SSTable::new(dir)?;
        for entry in self.memtable.get_entries_reversed().into_iter() {
            // problem is that database own memtable, into_iter
            // would move it, maybe need to create Iterator without
            // Into?
            match entry.value {
                Some(value) => {
                    sstable.set(entry.key.as_slice(), value.as_slice(), entry.timestamp)?
                }
                None => sstable.delete(entry.key.as_slice(), entry.timestamp)?,
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Database {
        let path = Path::new("data");
        Database::new(path).unwrap()
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
        let path = Path::new("data");
        let mut sstable = SSTable::new(&path).unwrap();
        db.set(key.as_slice(), value.as_slice(), timestamp).ok();
        sstable
            .set(key.as_slice(), value.as_slice(), timestamp)
            .ok();
        sstable.flush().ok();
        db.flush(path).ok();
        let item = sstable.get(key.as_slice()).unwrap();
        assert_eq!(vec![1], item.unwrap().value.unwrap());
    }
}
