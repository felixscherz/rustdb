use std::{
    cmp::{self, Ordering},
    io::{self, Result},
    path::Path,
};

use super::{iterator::SSTableEntry, sstable::SSTable};

// from https://codereview.stackexchange.com/questions/233872/writing-slice-compare-in-a-more-compact-way
// fn compare(a: &[u8], b: &[u8]) -> cmp::Ordering {
//     for (ai, bi) in a.iter().zip(b.iter()) {
//         match ai.cmp(&bi) {
//             Ordering::Equal => continue,
//             ord => return ord,
//         }
//     }
//     a.len().cmp(&b.len())
// }

impl SSTable {
    fn write_set(&mut self, entry: SSTableEntry) -> io::Result<()> {
        self.set(
            entry.key.as_slice(),
            entry.value.unwrap().as_slice(),
            entry.timestamp,
        )
    }

    fn write_delete(&mut self, entry: SSTableEntry) -> io::Result<()> {
        self.delete(entry.key.as_slice(), entry.timestamp)
    }

    pub fn merge(self, other: SSTable, dir: &Path) -> Result<SSTable> {
        let mut merged = SSTable::new(dir)?;
        let mut iterator = self.into_iter();
        let mut other_iterator = other.into_iter();
        let mut iterator_next = iterator.next();
        let mut other_iterator_next = other_iterator.next();
        loop {
            (iterator_next, other_iterator_next) = match (iterator_next, other_iterator_next) {
                (None, None) => (None, None), // both iterators are empty
                (Some(entry), None) => {
                    match entry.deleted {
                        false => merged.write_set(entry)?,
                        true => merged.write_delete(entry)?,
                    };
                    (iterator.next(), None)
                }
                (None, Some(entry)) => {
                    match entry.deleted {
                        false => merged.write_set(entry)?,
                        true => merged.write_delete(entry)?,
                    };
                    (None, other_iterator.next())
                }

                (Some(entry), Some(other_entry)) => {
                    match entry.key.as_slice().cmp(other_entry.key.as_slice()) {
                        Ordering::Less => {
                            match entry.deleted {
                                false => merged.write_set(entry)?,
                                true => merged.write_delete(entry)?,
                            };
                            (iterator.next(), Some(other_entry))
                        }
                        Ordering::Greater => {
                            match entry.deleted {
                                false => merged.write_set(other_entry)?,
                                true => merged.write_delete(other_entry)?,
                            };
                            (Some(entry), other_iterator.next())
                        }
                        Ordering::Equal => match entry.timestamp.cmp(&other_entry.timestamp) {
                            Ordering::Greater => {
                                match entry.deleted {
                                    false => merged.write_set(entry)?,
                                    true => (),
                                };
                                (iterator.next(), other_iterator.next())
                            }
                            Ordering::Less => {
                                match other_entry.deleted {
                                    false => merged.write_set(other_entry)?,
                                    true => (),
                                };
                                (iterator.next(), other_iterator.next())
                            }
                            Ordering::Equal => {
                                panic!("timestamps should not be equal between two items")
                            }
                        },
                    }
                }
            };
            if let (None, None) = (&iterator_next, &other_iterator_next) {
                break;
            }
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn create_path() -> PathBuf {
        PathBuf::from("data")
    }

    fn create_sstable(path: &Path) -> SSTable {
        SSTable::new(path).unwrap()
    }

    fn create_sstable_entry(key: Vec<u8>, timestamp: u128, deleted: bool) -> SSTableEntry {
        SSTableEntry {
            key,
            value: Some(vec![9]),
            timestamp,
            deleted,
        }
    }

    #[test]
    fn test_deleted_records_no_longer_in_sstable() {
        let path = create_path();
        let entry = create_sstable_entry(vec![1], 0, false);
        let mut sstable_a = create_sstable(&path);
        sstable_a.write_set(entry).ok();
        let mut sstable_b = create_sstable(&path);
        let entry = create_sstable_entry(vec![1], 1, true);
        sstable_b.write_delete(entry).ok();
        let merged = sstable_a.merge(sstable_b, &path).ok().unwrap();
        assert_eq!(merged.into_iter().count(), 0);
    }

    #[test]
    fn test_records_are_merged_in_order() {
        let path = create_path();
        let mut sstable_a = create_sstable(&path);
        for i in (1..10).step_by(2) {
           let entry = create_sstable_entry(vec![i], i.into(), false); 
           sstable_a.write_set(entry).ok();
        }
        let mut sstable_b = create_sstable(&path);
        for i in (0..9).step_by(2) {
           let entry = create_sstable_entry(vec![i], i.into(), false); 
           sstable_b.write_set(entry).ok();
        }
        let merged = sstable_a.merge(sstable_b, &path).ok().unwrap();
        for (i, entry) in merged.into_iter().enumerate() {
            assert_eq!(i, usize::try_from(entry.timestamp).unwrap())
        }


    }
}
