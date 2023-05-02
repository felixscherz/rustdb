use std::{
    fs::{read_dir, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::database::entry::Entry;

use super::iterator::{SSTableEntry, SSTableIterator};
use super::{
    data::{Data, DataIterator},
    index::Index,
};

// +---------------+---------------+-----------------+-...-+--...--+-----------------+
// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
// +---------------+---------------+-----------------+-...-+--...--+-----------------+

const BLOCK_SIZE: usize = 65536;

pub struct SSTable {
    pub path: PathBuf,
    data: Data,
    index: Index,
    file: BufWriter<File>,
    current_block_size: usize,
}

impl IntoIterator for SSTable {
    type IntoIter = SSTableIterator;
    type Item = SSTableEntry;

    fn into_iter(self) -> SSTableIterator {
        SSTableIterator::new(self.path).unwrap()
    }
}

impl SSTable {
    pub fn new(dir: &Path) -> io::Result<SSTable> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".sstable");
        let data_path = Path::new(dir).join(timestamp.to_string() + ".data.sstable");
        let index_path = Path::new(dir).join(timestamp.to_string() + ".index.sstable");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        let current_block_size = 0;
        let data = Data::new(&data_path)?;
        let index = Index::new(&index_path)?;

        Ok(SSTable {
            path,
            data,
            index,
            file,
            current_block_size,
        })
    }

    pub fn from_path(path: &Path) -> io::Result<SSTable> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        let current_block_size = 0;
        let binding = path.clone().to_path_buf();
        let data_path = binding
            .to_str()
            .unwrap()
            .replace(".sstable", ".data.sstable");
        let index_path = binding
            .to_str()
            .unwrap()
            .replace(".sstable", ".index.sstable");
        let data_path = Path::new(&data_path);
        let data = Data::from_path(&data_path)?;
        let index_path = Path::new(&index_path);
        let index = Index::from_path(&index_path)?;
        Ok(SSTable {
            path: path.to_owned(),
            data,
            index,
            file,
            current_block_size,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        let entry_size = size(key, Some(value), timestamp);
        let entry = Entry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            deleted: false,
            timestamp,
        };
        if self.current_block_size == 0 || self.current_block_size + entry_size > BLOCK_SIZE {
            let offset = self.data.get_offset();
            self.index.write(&entry, offset)?;
            self.current_block_size = 0;
            // write this item to index
        }
        self.current_block_size += entry_size;
        self.data.write(&entry)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        let entry_size = size(key, None, timestamp);
        let entry = Entry {
            key: key.to_vec(),
            value: None,
            deleted: true,
            timestamp,
        };
        if self.current_block_size + entry_size > BLOCK_SIZE {
            let offset = self.data.get_offset();
            self.index.write(&entry, offset)?;
            self.current_block_size = 0;
        }
        self.data.write(&entry)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.data.flush()
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<SSTableEntry>> {
        // simply go through entire sstable
        let iterator = DataIterator::new(self.data.path.clone())?;
        for entry in iterator {
            if entry.key.as_slice() == key {
                return Ok(Some(SSTableEntry {
                    key: entry.key,
                    value: entry.value,
                    timestamp: entry.timestamp,
                    deleted: entry.deleted,
                }));
            }
        }
        Ok(None)
    }
}

fn size(key: &[u8], value: Option<&[u8]>, timestamp: u128) -> usize {
    let boolean_size = 1;
    let size_in_bytes =
        key.len() + key.len().to_le_bytes().len() + timestamp.to_le_bytes().len() + boolean_size;
    if let Some(val) = value {
        size_in_bytes + val.len() + val.len().to_le_bytes().len()
    } else {
        size_in_bytes
    }
}

pub fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }
    files
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_entry_from_sstable() {
        let mut sstable = create_sstable().unwrap();
        let entry = create_entry();
        sstable
            .set(
                entry.key.as_slice(),
                entry.value.unwrap().as_slice(),
                entry.timestamp,
            )
            .unwrap();
        sstable.flush().unwrap();
        let return_value = sstable.get(entry.key.as_slice()).unwrap();
        assert!(return_value.is_some());
        assert_eq!(return_value.unwrap().key, entry.key);
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

    fn create_sstable() -> io::Result<SSTable> {
        let path = create_path();
        SSTable::new(&path)
    }
}
