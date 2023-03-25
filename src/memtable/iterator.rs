use super::memtable::MemTableEntry;

pub struct MemTableIterator {
    entries: Vec<MemTableEntry>,
}

impl MemTableIterator {
    pub fn new(entries: Vec<MemTableEntry>) -> Self {
        MemTableIterator { entries }
    }
}

impl Iterator for MemTableIterator {
    type Item = MemTableEntry;

    fn next(&mut self) -> Option<MemTableEntry> {
        // pop first entry from entries and decrease size accordingly
        self.entries.pop()
    }
}
