# classes

* `MemTable`
* `WAL`

# Todo


## sstable

* when memtable size exceeds a certain number of bytes flush it to sstable
* it's already in order
* string tables need an index file associated with it for searching

### implementation

* write memtable in order similar to wal to file
* implement into_iter for memtable -> then for every item use `SSTable.set`
* for item in memtable:
    * write same format (keylen, tombstone, valuelen, key, value, timestamp)
    * memtable is already ordered by key


#### implement combining two sstables:
* read item from both
* compare them
* write lower one and read next from that table
* repeat

### index
* index needs to contain mapping from key to byte offset where that element begins in sstable
* only save a couple of keys

#### performance
* implement a bloom filter in order to skip searching an sstable

