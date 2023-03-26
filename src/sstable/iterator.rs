use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::{self, Read};
use std::path::PathBuf;

#[derive(Debug)]
pub struct SSTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

pub struct SSTableIterator {
    /// will turn into iterator over sstable
    /// needs to have access to a file in the form of `BufReader`
    reader: BufReader<File>,
}

impl SSTableIterator {
    pub fn new(path: PathBuf) -> io::Result<SSTableIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(SSTableIterator { reader })
    }
}
// +---------------+---------------+-----------------+-...-+--...--+-----------------+
// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
// +---------------+---------------+-----------------+-...-+--...--+-----------------+

impl Iterator for SSTableIterator {
    type Item = SSTableEntry; // item that is returned by `next` method

    fn next(&mut self) -> Option<SSTableEntry> {
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
        Some(SSTableEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}
