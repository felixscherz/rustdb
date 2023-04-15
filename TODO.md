# Todo

## sstable

* string tables need an index file associated with it for searching

### index

* index needs to contain mapping from key to byte offset where that element begins in sstable
* only save a couple of keys

#### implementation

* next to sstable save and sstable_index file that is quicker to load into memory
* do this while writing individual items to sstable
* layout would be `| key size | key | byte offset |`
* then read would be:
    * load full sstable index
    * lookup key with binary search to find position between index entries
    * if exact match -> use byte offset to start reading
    * if possibly between two index elements -> use their byte offsets as start offset and end offset


### performance

* implement a bloom filter in order to skip searching an sstable


