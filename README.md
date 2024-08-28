# PebbleDB

PebbleDB is a persistent key-value store based on an LSM-tree.
It mimics the well-known [RocksDB](https://rocksdb.org/) ([source code](https://github.com/facebook/rocksdb)).

I did it for educational purposes only: I wanted to get a deeper understanding of the core principles and the major
components behind LSM-trees.

## Getting started

```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual env
source .venv/bin/activate

# Install the dependencies
python3 -m pip install -r requirements.txt

# Run tests
pytest
```

## Features and implementation notes

### Main components

**Components used for storage:**

- **Red-black tree**: a self-balancing binary tree that stores all the records that were inserted recently (until
  they are big enough to be written to disk). Therefore, records are stored in key order and are retrievable in
  logarithmic time (since the height of the tree is `log(number_nodes)`). One node corresponds to one key (if there are
  multiple versions, they are all stored in the same node). It is agnostic to the nature of data passed to it (it stores
  keys and values as bytes).
- **Memtable**: the in-memory component of the LSM-tree. It is implemented as a wrapper around the red-black tree
  with some extra work related to understanding the nature of the data (keeping track of the size of data stored in
  memory, dealing with the WAL, selecting the correct version when reading from the red-black tree, ...). At one given
  time, there can be multiple memtables (waiting to be flushed to disk), but there is only one to which data is
  inserted.
- **SSTable**: the on-disk component of the LSM-tree. There are multiple SSTables in the storage engine. Each
  SSTable corresponds physically to one file on disk. SSTables are organized in levels (depending on the number of
  compaction operations that they have been through). Each SSTable is further broken down in a number of
  **data blocks**. Each data block has the size of an SSD page and are thus the flushing unit. Each SSTable also has one
  **meta block** that stores some metadata (offsets corresponding to each data block, number of blocks, ...).

**Recovery components:**

- **Write-Ahead Log (WAL)**: a log file in which records are written before being added to the memtable. If the
  database crashes, the content of each memtable can be retrieved from the WAL.
- **Manifest**: a log file that stores all internal operations that modify the contents of SSTables
  (compaction, flush). Since the operations are recorded in order, it can be used to recreate the state of the engine
  upon restart after a crash.

**Helper components:**

- **Read-write lock**: used to handle concurrent requests and protect resources. It allows only one thread to write
  data to memtables and SSTables, but multiple threads can read their content concurrently if none is writing to it.
- **Bloom filter**: a space-efficient probabilistic data structure that allows to predict whether an item may or may not
  be in a collection (SSTable in the case of this engine). This is an optimization on I/O operations: SSTables are
  read only if they are likely to contain the searched item.
- **Sequence number generator**: generates unique and monotonically increasing sequence numbers that are added to each
  record (and useful for multi-version concurrency control). Implemented as a singleton to guarantee unicity and
  monotonically increasing increments even if it is called from multiple places.
- **Iterators**: objects that provide access to items in order and one at a time. There is one for each storage
  component (memtable, SSTable, data block) and different flavors of those depending on the use (scan query, compaction,
  flush).

### Main operations

**Interacting with the database:**

- `get`: looks up the record in storage components in the relevant order (active memtable, then frozen memtables, then
  SSTables in increasing levels).
- `put`: inserts a record in the active memtable. Inserted records later cascade through all components during their
  lifecycle.
- `scan`: performs a scan query by iterating over all storage components at once (active memtable, immutable memtables
  and SSTables).

**Database's internal operations:**

- **Freeze**: makes the active memtable immutable once it is full.
- **Flush**: flushes the content of an immutable memtable to disk so that they are persisted on non-volatile storage.
- **Compaction**: discards obsolete records from SSTables to reclaim disk space.

**Recovery:** reconstructs the state by reading the manifest file (to know which files on disk should be referenced from
memory) and by reading the WAL (to recover the content of memtables that had not yet been flushed to disk).

**Transactional layer and snapshot isolation:** snapshot isolation guarantees that a given read request reads a
consistent snapshot of the database.
In practice, snapshot isolation is implemented via multi-version concurrency control (MVCC):
the engine adds a monotonically increasing sequence number to each record so that multiple versions can be stored
simultaneously.
The sequence number of the last record inserted is kept in the state and is used for read queries (to make sure that we
read the most recent record that was written before or at this snapshot) and for compaction (to make sure only obsolete
records - the ones that we are sure will never be used again - are discarded).




