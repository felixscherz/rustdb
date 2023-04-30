use crate::database::entry::Entry;
use std::fs::OpenOptions;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::{fs::File, io::BufWriter, io::Read, io::Write};

pub struct Data {
    pub path: PathBuf,
    file: BufWriter<File>,
}

pub struct DataIterator {
    reader: BufReader<File>,
}

impl DataIterator {
    pub fn new(path: PathBuf) -> io::Result<DataIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(DataIterator { reader })
    }
}

impl Iterator for DataIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        Entry::read(&mut self.reader)
    }
}

impl Data {
    pub fn new(path: &Path) -> io::Result<Data> {
        Self::from_path(path)
    }

    pub fn from_path(path: &Path) -> io::Result<Data> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(Data {
            path: path.to_owned(),
            file,
        })
    }
    pub fn write(&mut self, entry: &Entry) -> io::Result<()> {
        entry.write(&mut self.file)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Entry>> {
        // simply go through entire sstable
        let iterator = DataIterator::new(self.path.clone())?;
        for entry in iterator {
            if entry.key.as_slice() == key {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

impl Entry {
    pub fn write(&self, file: &mut BufWriter<File>) -> io::Result<()> {
        file.write_all(&self.key.len().to_le_bytes())?;
        file.write_all(&(self.deleted as u8).to_le_bytes())?;
        if let Some(val) = &self.value {
            file.write_all(&val.len().to_le_bytes())?;
            file.write_all(&self.key)?;
            file.write_all(&val)?;
        } else {
            file.write_all(&self.key)?;
        }
        file.write_all(&self.timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn read(file: &mut BufReader<File>) -> Option<Entry> {
        // reads the sequence of bytes corresponding to an SSTableEntry from the file
        let mut len_buffer = [0; 8]; // key length is encoded in first 8 bytes
        if file.read_exact(&mut len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer); // turn bytes into usize to get key length
        let mut bool_buffer = [0; 1];
        // read next byte to get tombstone byte
        if file.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;
        let mut key = vec![0; key_len];
        let mut value: Option<Vec<u8>> = None;
        if deleted {
            // if deleted, then value_len and value don't exist -> next bytes are key bytes
            if file.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            // read the next 8 bytes to get value length as bytes
            if file.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            // read next key_len bytes to get key
            if file.read_exact(&mut key).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            // read next value_len bytes to get value
            if file.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf)
        }
        let mut timestamp_buffer = [0; 16];
        if file.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);
        Some(Entry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_get_entry_from_data() {
        let mut data = create_data().unwrap();
        let entry = create_entry();
        data.write(&entry).unwrap();
        data.flush().unwrap();
        let return_value = data.get(&entry.key.as_slice()).unwrap();
        assert!(return_value.is_some());
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

    fn create_data() -> io::Result<Data> {
        let path = create_path();
        let timestamp = create_timestamp();
        Data::new(&path.join(timestamp.to_string() + "data.sstable"))
    }
}
