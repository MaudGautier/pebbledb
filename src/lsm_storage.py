import os
import time
from collections import deque
from typing import Optional, Iterator

from src.iterators import MemTableIterator, MergingIterator, SSTableIterator
from src.locks import ReadWriteLock, Mutex
from src.memtable import MemTable
from src.record import Record
from src.sstable import SSTableBuilder, SSTable


class LsmStorage:
    def __init__(self,
                 max_sstable_size: Optional[int] = 262_144_000,
                 block_size: Optional[int] = 65_536,
                 directory: Optional[str] = "."):
        self.memtable = self._create_memtable()
        # Immutable memtables are stored in a linked list because they will always be parsed from most recent to oldest.
        self.immutable_memtables: deque[MemTable] = deque()
        self._read_write_lock = ReadWriteLock()
        self._state_lock = Mutex()
        self._max_sstable_size = max_sstable_size
        self._block_size = block_size
        self.ss_tables: deque[SSTable] = deque()
        self.ss_tables_levels = []
        self.directory = directory
        self._create_directory()

    @staticmethod
    def _create_memtable():
        return MemTable()

    def _try_freeze(self):
        """Checks if the memtable should be frozen or not.
        The memtable should be frozen if it is bigger than the `self._max_sstable_size` threshold.

        Further explanations on the details of this method:
        - It acquires the `self._state_lock` to ensure that only one freeze operation occurs at any given time
          (necessary because concurrency is allowed => two concurrent insert operations could end up doing a freeze)
        - The size of the memtable is checked twice: once before acquiring the `self._state_lock` and a second time
          after acquiring it. This is to avoid taking a lock unnecessarily the first time and thus to avoid impacting
          performance (preventing other operations needing this lock when it is not necessary). Once the condition is
          true and the `self._state_lock` is acquired, it is important to check the condition a second time because the
          memtable might have been frozen by another operation while this one was checking the condition and acquiring
          the lock.
        - It acquires a read lock on the state before reading the memtable's size. This is in order to comply to the
          concurrency protocol here: the mutex `self._state_lock` is for freeze operations (and other operations
          modifying the state), the ReadWriteLock `self._read_write_lock` is for reading/writing on one element.

        Note: another approach would have been to do the following:
        ```
        if approximate_size >= self._max_sstable_size:
            with self._state_lock:
                state_read_lock = self._read_write_lock.read()
                state_read_lock.__enter__()
                latest_approximate_size = self.memtable.approximate_size
                if latest_approximate_size >= self._max_sstable_size:
                    state_read_lock.__exit__()
                    self._force_freeze_memtable()
                else:
                    state_read_lock.__exit__()
        ```
        In that second approach, the read/write lock is explicitly managed and its release is made _after_ having
        checked the condition (vs in the current implementation, it is released before making the check).
        The trade-off between the two is about concurrency and atomicity:
        - Current implementation: better concurrency, decreased atomicity (a write operation could add/delete records
          and thus change the size of the memtable while the condition is being checked)
        - Alternative implementation: better atomicity, decreased concurrency (the lock is held longer => that may
          prevent other operations to write).

        Given that this is only an approximate size, and it does not matter much if it is slightly bigger or smaller, I
        opted for better concurrency here (it is no big deal if the size is a bit bigger or smaller when freezing than
        when checked).
        """
        with self._read_write_lock.read():
            approximate_size = self.memtable.approximate_size

        if approximate_size >= self._max_sstable_size:
            with self._state_lock:
                with self._read_write_lock.read():
                    latest_approximate_size = self.memtable.approximate_size
                if latest_approximate_size >= self._max_sstable_size:
                    self._freeze_memtable()

    def _freeze_memtable(self):
        with self._read_write_lock.write():
            new_memtable = self._create_memtable()
            self.immutable_memtables.insert(0, self.memtable)
            self.memtable = new_memtable

    def put(self, key: Record.Key, value: Record.Value):
        self.memtable.put(key=key, value=value)
        self._try_freeze()

    def get(self, key: Record.Key) -> Optional[Record.Value]:
        value = self.memtable.get(key=key)

        if value is not None:
            return value

        for memtable in self.immutable_memtables:
            value = memtable.get(key=key)
            if value is not None:
                return value

        for sstable in self.ss_tables:
            if not sstable.bloom_filter.may_contain(key=key):
                continue
            value = sstable.get(key=key)
            if value is not None:
                return value

        for level in self.ss_tables_levels:
            for sstable in level:
                if not sstable.first_key <= key <= sstable.last_key:
                    continue
                if not sstable.bloom_filter.may_contain(key=key):
                    continue
                value = sstable.get(key=key)
                if value is not None:
                    return value

        return None

    def scan(self, lower: Record.Key, upper: Record.Key) -> Iterator[Record]:
        active_memtable_iterator = self.memtable.scan(lower=lower, upper=upper)
        immutable_memtables_iterators = [memtable.scan(lower=lower, upper=upper) for memtable in
                                         self.immutable_memtables]
        sstables_iterators = [sstable.scan(lower=lower, upper=upper) for sstable in self.ss_tables]

        iterator = MergingIterator(
            iterators=[active_memtable_iterator] + immutable_memtables_iterators + sstables_iterators)
        yield from iterator

    def flush_next_immutable_memtable(self) -> None:
        with self._state_lock:
            # Read the oldest memtable
            with self._read_write_lock.read():
                memtable_to_flush = self.immutable_memtables[-1]

            # Flush it to SSTable
            path = self._compute_path()
            sstable_builder = SSTableBuilder(sstable_size=self._max_sstable_size, block_size=self._block_size)
            memtable_iterator = MemTableIterator(memtable=memtable_to_flush)
            for record in memtable_iterator:
                sstable_builder.add(key=record.key, value=record.value)
            sstable = sstable_builder.build(path=path)

            # Update state to remove oldest memtable and add new SSTable
            with self._read_write_lock.write():
                self.immutable_memtables.pop()
                self.ss_tables.insert(0, sstable)

    def _compute_path(self):
        timestamp_in_us = int(time.time() * 1_000_000)
        return f"{self.directory}/{timestamp_in_us}.sst"

    def _create_directory(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def _compact(self, sstables_iterator: MergingIterator) -> list[SSTable]:
        new_ss_tables = []
        sstable_builder = SSTableBuilder(sstable_size=self._max_sstable_size, block_size=self._block_size)
        for record in sstables_iterator:
            sstable_builder.add(key=record.key, value=record.value)
            if sstable_builder.current_buffer_position >= self._max_sstable_size:
                sstable = sstable_builder.build(path=self._compute_path())
                new_ss_tables.append(sstable)
                sstable_builder = SSTableBuilder(sstable_size=self._max_sstable_size, block_size=self._block_size)
        if sstable_builder.keys:
            sstable = sstable_builder.build(path=self._compute_path())
            new_ss_tables.append(sstable)

        return new_ss_tables

    def trigger_compaction(self):
        # For now: compacts all L0 sstables into L1 sstables
        with self._read_write_lock.read():
            sstables_to_compact = [sstable for sstable in self.ss_tables]
            l0_ss_table_iterator = MergingIterator(iterators=[
                SSTableIterator(sstable=sstable) for sstable in sstables_to_compact
            ])

        new_ss_tables = self._compact(sstables_iterator=l0_ss_table_iterator)

        with self._state_lock:
            with self._read_write_lock.write():
                self.ss_tables_levels.insert(0, new_ss_tables)
                for sstable in sstables_to_compact:
                    self.ss_tables.remove(sstable)
                # TODO: update this to deal with more levels (when the time comes)
