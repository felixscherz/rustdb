use std::{
    fs::{read_dir, File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use super::iterator::{SSTableEntry, SSTableIterator};

// +---------------+---------------+-----------------+-...-+--...--+-----------------+
// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
// +---------------+---------------+-----------------+-...-+--...--+-----------------+

pub struct SSTable {
    pub path: PathBuf,
    file: BufWriter<File>,
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

        Ok(SSTable { path, file })
    }

    pub fn from_path(path: &Path) -> io::Result<SSTable> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(SSTable {
            path: path.to_owned(),
            file,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
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
                return Ok(Some(entry))
            }
        }
        Ok(None)
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
