use std::{
    fs::{read_dir, remove_file, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::memtable::MemTable;

use super::iterator::{WALEntry, WALIterator};

pub struct WAL {
    path: PathBuf,
    file: BufWriter<File>,
}

impl IntoIterator for WAL {
    type IntoIter = WALIterator;
    type Item = WALEntry;

    fn into_iter(self) -> WALIterator {
        WALIterator::new(self.path).unwrap()
    }
}

impl WAL {
    pub fn new(dir: &Path) -> io::Result<WAL> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL { path, file })
    }
    pub fn from_path(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL {
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

    pub fn load_from_dir(dir: &Path) -> io::Result<(WAL, MemTable)> {
        let mut wal_files = files_with_ext(dir, "wal");
        wal_files.sort();

        let mut new_mem_table = MemTable::new();
        let mut new_wal = WAL::new(dir)?;
        for wal_file in wal_files.iter() {
            if let Ok(wal) = WAL::from_path(wal_file) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
                        new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    } else {
                        new_mem_table.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        );
                        new_wal.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        )?;
                    }
                }
            }
        }
        new_wal.flush().unwrap();
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());
        Ok((new_wal, new_mem_table))
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

    fn create_wal() -> io::Result<WAL> {
        let path = Path::new("data");
        WAL::new(&path)
    }

    fn create_entry() -> WALEntry {
        WALEntry {
            key: vec![1, 2, 3],
            value: Some(vec![9]),
            timestamp: 1,
            deleted: false,
        }
    }

    fn write_to_wal(wal: &mut WAL, entry: WALEntry) -> io::Result<()> {
        wal.set(
            entry.key.as_slice(),
            entry.value.unwrap().as_slice(),
            entry.timestamp,
        )
    }

    #[test]
    fn test_write_to_wal() -> io::Result<()> {
        let mut wal = create_wal().unwrap();
        let entry = create_entry();
        write_to_wal(&mut wal, entry)
    }

}
