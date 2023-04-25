use std::{
    fs::{read_dir, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use super::data::Data;
use super::iterator::{SSTableEntry, SSTableIterator};

// +---------------+---------------+-----------------+-...-+--...--+-----------------+
// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
// +---------------+---------------+-----------------+-...-+--...--+-----------------+

const BLOCK_SIZE: usize = 65536;

pub struct SSTable {
    pub path: PathBuf,
    data: Data,
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
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        let current_block_size = 0;
        let data = Data::new(dir)?;

        Ok(SSTable {
            path,
            data,
            file,
            current_block_size,
        })
    }

    pub fn from_path(path: &Path) -> io::Result<SSTable> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        let current_block_size = 0;
        let data = Data::from_path(path)?;
        Ok(SSTable {
            path: path.to_owned(),
            data,
            file,
            current_block_size,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        let entry_size = size(key, Some(value), timestamp);
        if self.current_block_size == 0 || self.current_block_size + entry_size > BLOCK_SIZE {
            self.current_block_size = 0;
            // write this item to index
        }
        self.current_block_size += entry_size;

        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        let entry_size = size(key, None, timestamp);
        if self.current_block_size + entry_size > BLOCK_SIZE {
            self.current_block_size = 0;
        }
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<SSTableEntry>> {
        // simply go through entire sstable
        let iterator = SSTableIterator::new(self.path.clone())?;
        for entry in iterator {
            if entry.key.as_slice() == key {
                return Ok(Some(entry));
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
    fn nothing() {
        assert!(true)
    }
}
