use crate::database::entry::Entry;
use std::fs::OpenOptions;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::{fs::File, io::BufWriter, io::Read, io::Write};

const USIZE_LEN: usize = std::mem::size_of::<usize>();

pub struct Index {
    pub path: PathBuf,
    file: BufWriter<File>,
}

pub struct IndexIterator {
    reader: BufReader<File>,
}

pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: usize,
}

impl IndexIterator {
    pub fn new(path: PathBuf) -> io::Result<IndexIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(IndexIterator { reader })
    }
}

impl Iterator for IndexIterator {
    type Item = IndexEntry;

    fn next(&mut self) -> Option<IndexEntry> {
        Index::read(&mut self.reader)
    }
}

impl Index {
    pub fn new(path: &Path) -> io::Result<Index> {
        Self::from_path(path)
    }

    pub fn from_path(path: &Path) -> io::Result<Index> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(Index {
            path: path.to_owned(),
            file,
        })
    }
    pub fn write(&mut self, entry: &Entry, offset: usize) -> io::Result<()> {
        self.file.write_all(&entry.key.len().to_le_bytes())?;
        self.file.write_all(&entry.key)?;
        self.file.write_all(&offset.to_le_bytes())?;
        Ok(())
    }

    pub fn read(file: &mut BufReader<File>) -> Option<IndexEntry> {
        let mut len_buffer = [0; 8];
        if file.read_exact(&mut len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer);
        let mut key = vec![0; key_len];
        if file.read_exact(&mut key).is_err() {
            return None;
        }

        let mut offset_buffer = [0; USIZE_LEN];
        if file.read_exact(&mut offset_buffer).is_err() {
            return None;
        }
        let offset = usize::from_le_bytes(offset_buffer);
        Some(IndexEntry { key, offset })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<usize>> {
        let iterator = IndexIterator::new(self.path.clone())?;
        for entry in iterator {
            if entry.key.as_slice() == key {
                return Ok(Some(entry.offset));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    #[test]
    fn test_get_offset_from_index() {
        let mut index = create_index().unwrap();
        let entry = create_entry();
        index.write(&entry, 0).unwrap();
        index.flush().unwrap();
        let result_offset = index.get(&entry.key.as_slice()).unwrap();
        assert!(result_offset.is_some());
    }
    fn create_entry() -> Entry {
        Entry {
            key: vec![1, 2, 3],
            value: Some(vec![9]),
            timestamp: 1,
            deleted: false,
        }
    }

    fn create_path() -> PathBuf {
        PathBuf::from("data")
    }

    fn create_timestamp() -> u128 {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        timestamp
    }

    fn create_index() -> io::Result<Index> {
        let path = create_path();
        let timestamp = create_timestamp();
        Index::new(&path.join(timestamp.to_string() + ".index.sstable"))
    }
}
