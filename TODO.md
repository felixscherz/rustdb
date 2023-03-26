# Todo

## sstable

* when memtable size exceeds a certain number of bytes flush it to sstable
* string tables need an index file associated with it for searching

### combining two sstables:
* read item from both
* compare them
* write lower one and read next from that table
* repeat

### index
* index needs to contain mapping from key to byte offset where that element begins in sstable
* only save a couple of keys

### performance
* implement a bloom filter in order to skip searching an sstable


