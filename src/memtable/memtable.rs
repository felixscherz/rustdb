use super::iterator::MemTableIterator;

pub struct MemTable {
    entries: Vec<MemTableEntry>,
    pub size: usize,
}

#[derive(Clone, Debug)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

impl IntoIterator for MemTable {
    type IntoIter = MemTableIterator;
    type Item = MemTableEntry;

    fn into_iter(self) -> MemTableIterator {
        MemTableIterator::new(self.get_entries_reversed())
    }
}

impl MemTable {
    pub fn get_entries_reversed(&self) -> Vec<MemTableEntry> {
        let mut entries = self.entries.clone();
        entries.reverse();
        entries
    }
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            entries: Vec::new(),
            size: 0,
        }
    }

    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(v) = self.entries[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                let timestamp_size = 16;
                let boolean_size = 1;
                self.size += key.len() + value.len() + timestamp_size + boolean_size;
                self.entries.insert(idx, entry);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }
        None
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };
        match self.get_index(key) {
            Ok(idx) => {
                if let Some(v) = self.entries[idx].value.as_ref() {
                    self.size -= v.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                let timestamp_size = 16;
                let boolean_size = 1;
                self.size += key.len() + timestamp_size + boolean_size;
                self.entries.insert(idx, entry);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn create_memtable() {
        let table: MemTable = MemTable::new();
    }

    pub fn prepare_memtable() -> MemTable {
        let entries: Vec<MemTableEntry> = (0..10)
            .map(|i| MemTableEntry {
                key: vec![i],
                value: Some(vec![i]),
                timestamp: 12,
                deleted: false,
            })
            .collect();
        let table: MemTable = MemTable {
            size: entries.len(),
            entries,
        };
        table
    }

    #[test]
    fn do_search() {
        let table = prepare_memtable();
        let res = table.get_index(&vec![2][..]);
        assert!(res.is_ok());
    }

    #[test]
    fn do_iter() {
        let table = prepare_memtable();
        for entry in table.into_iter() {
            assert_eq!(entry.timestamp, 12)
        }
    }
}
