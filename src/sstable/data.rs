use crate::database::entry::Entry;
use std::fs::OpenOptions;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::{fs::File, io::BufWriter, io::Read, io::Write};

pub struct Data {
    pub path: PathBuf,
    file: BufWriter<File>,
    offset: usize,
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
        // reads the sequence of bytes corresponding to an SSTableEntry from the file
        let mut len_buffer = [0; 8]; // key length is encoded in first 8 bytes
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer); // turn bytes into usize to get key length
        let mut bool_buffer = [0; 1];
        // read next byte to get tombstone byte
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;
        let mut key = vec![0; key_len];
        let mut value: Option<Vec<u8>> = None;
        if deleted {
            // if deleted, then value_len and value don't exist -> next bytes are key bytes
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            // read the next 8 bytes to get value length as bytes
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            // read next key_len bytes to get key
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            // read next value_len bytes to get value
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf)
        }
        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
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

impl Data {
    pub fn new(path: &Path) -> io::Result<Data> {
        Self::from_path(path)
    }

    pub fn from_path(path: &Path) -> io::Result<Data> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let offset: usize = file.metadata().unwrap().len().try_into().unwrap();
        let file = BufWriter::new(file);
        Ok(Data {
            path: path.to_owned(),
            file,
            offset,
        })
    }
    pub fn write(&mut self, entry: &Entry) -> io::Result<()> {
        self.file.write_all(&entry.key.len().to_le_bytes())?;
        self.file.write_all(&(entry.deleted as u8).to_le_bytes())?;
        if let Some(val) = &entry.value {
            self.file.write_all(&val.len().to_le_bytes())?;
            self.file.write_all(&entry.key)?;
            self.file.write_all(&val)?;
        } else {
            self.file.write_all(&entry.key)?;
        }
        self.file.write_all(&entry.timestamp.to_le_bytes())?;
        self.offset += Self::size_of_entry(entry);
        Ok(())
    }

    fn size_of_entry(entry: &Entry) -> usize {
        let key_size = entry.key.len() + std::mem::size_of::<usize>();
        let value_size = match &entry.value {
            Some(val) => val.len() + std::mem::size_of::<usize>(),
            None => 0,
        };
        let deleted_size = std::mem::size_of::<bool>();
        let timestamp_size = std::mem::size_of::<u128>();
        key_size + value_size + deleted_size + timestamp_size
    }

    pub fn get_offset(&self) -> usize {
        self.offset
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
        Data::new(&path.join(timestamp.to_string() + ".data.sstable"))
    }

    #[test]
    fn test_size_of_entry() {
        let mut data = create_data().unwrap();
        let entry = create_entry();
        data.write(&entry).unwrap();
        let offset = data.get_offset();
        let entry_size = std::mem::size_of::<usize>() * 2 + 16 + 1 + 3 + 1;
        println!("{}", std::mem::size_of::<usize>());
        assert_ne!(offset, 0);
        assert_eq!(offset, entry_size);
    }
}
