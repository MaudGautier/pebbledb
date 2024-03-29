import os
import time
from collections import deque
from typing import Optional, Iterator

from src.locks import ReadWriteLock, Mutex
from src.memtable import MemTable
from src.record import Record
from src.sstable import SSTableBuilder, SSTable
from src.utils import merge_iterators, filter_duplicate_keys


class LsmStorage:
    def __init__(self,
                 max_sstable_size: Optional[int] = 262_144_000,
                 block_size: Optional[int] = 65_536,
                 directory: Optional[str] = "."):
        self.memtable = self._create_memtable()
        # Immutable memtables are stored in a linked list because they will always be parsed from most recent to oldest.
        self.immutable_memtables: deque[MemTable] = deque()
        self._state_lock = ReadWriteLock()
        self._freeze_lock = Mutex()
        self._max_sstable_size = max_sstable_size
        self._block_size = block_size
        self.ss_tables_paths = []
        self.directory = directory
        self.create_directory()

    @staticmethod
    def _create_memtable():
        return MemTable()

    def _try_freeze(self):
        """Checks if the memtable should be frozen or not.
        The memtable should be frozen if it is bigger than the `self._max_sstable_size` threshold.

        Further explanations on the details of this method:
        - It acquires the `self._freeze_lock` to ensure that only one freeze operation occurs at any given time
          (necessary because concurrency is allowed => two concurrent insert operations could end up doing a freeze)
        - The size of the memtable is checked twice: once before acquiring the `self._freeze_lock` and a second time
          after acquiring it. This is to avoid taking a lock unnecessarily the first time and thus to avoid impacting
          performance (preventing other operations needing this lock when it is not necessary). Once the condition is
          true and the `self._freeze_lock` is acquired, it is important to check the condition a second time because the
          memtable might have been frozen by another operation while this one was checking the condition and acquiring
          the lock.
        - It acquires a read lock on the state before reading the memtable's size. This is in order to comply to the
          concurrency protocol here: the mutex `self._freeze_lock` is for freeze operations, the ReadWriteLock
          `self.state_lock` is for reading/writing on the state.

        Note: another approach would have been to do the following:
        ```
        if approximate_size >= self._max_sstable_size:
            with self._freeze_lock:
                state_read_lock = self._state_lock.read()
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
        with self._state_lock.read():
            approximate_size = self.memtable.approximate_size

        if approximate_size >= self._max_sstable_size:
            with self._freeze_lock:
                with self._state_lock.read():
                    latest_approximate_size = self.memtable.approximate_size
                if latest_approximate_size >= self._max_sstable_size:
                    self._freeze_memtable()

    def _freeze_memtable(self):
        with self._state_lock.write():
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

        for sstable_path in self.ss_tables_paths:
            sstable = SSTable.from_file(file_path=sstable_path)
            value = sstable.get(key=key)
            if value is not None:
                return value

        return None

    def scan(self, lower: Record.Key, upper: Record.Key) -> Iterator[Record]:
        active_memtable_iterator = self.memtable.scan(lower=lower, upper=upper)
        immutable_memtables_iterators = [memtable.scan(lower=lower, upper=upper) for memtable in
                                         self.immutable_memtables]

        merged = merge_iterators([active_memtable_iterator] + immutable_memtables_iterators)
        filtered = filter_duplicate_keys(merged)
        yield from filtered

    def flush_next_immutable_memtable(self) -> None:
        with self._freeze_lock:
            # Read the oldest memtable
            with self._state_lock.read():
                memtable_to_flush = self.immutable_memtables[-1]

            # Flush it to SSTable
            path = self.compute_path()
            sstable_builder = SSTableBuilder(sstable_size=self._max_sstable_size, block_size=self._block_size)
            for record in memtable_to_flush:
                sstable_builder.add(key=record.key, value=record.value)
            sstable = sstable_builder.build()
            sstable.write(path)

            # Update state to remove oldest memtable and add new SSTable
            with self._state_lock.write():
                self.immutable_memtables.pop()
                self.ss_tables_paths.append(path)

    def compute_path(self):
        timestamp_in_us = time.time() * 1_000_000
        return f"{self.directory}/{timestamp_in_us}.sst"

    def create_directory(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
