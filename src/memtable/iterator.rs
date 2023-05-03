use crate::database::entry::Entry;

pub struct MemTableIterator {
    entries: Vec<Entry>,
}

impl MemTableIterator {
    pub fn new(entries: Vec<Entry>) -> Self {
        MemTableIterator { entries }
    }
}

impl Iterator for MemTableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        // pop first entry from entries and decrease size accordingly
        self.entries.pop()
    }
}
