import os
import time
from collections import deque
from typing import Optional, Iterator, Deque, Type

from src.iterators import MemTableIterator, MergingIterator, SSTableIterator, ConcatenatingIterator, BaseIterator
from src.locks import ReadWriteLock, Mutex
from src.manifest import Manifest, Configuration, FlushEvent, CompactionEvent
from src.memtable import MemTable
from src.record import Record
from src.sstable import SSTableBuilder, SSTable


class LsmState:
    def __init__(self,
                 memtable: MemTable,
                 immutable_memtables: Deque[MemTable],
                 sstables_level0: Deque[SSTable],
                 sstables_levels: list[Deque[SSTable]]):
        self.memtable = memtable
        self.immutable_memtables = immutable_memtables
        self.sstables_level0 = sstables_level0
        self.sstables_levels = sstables_levels


class LsmLocks:
    def __init__(self):
        self.read_write = ReadWriteLock()
        self.state = Mutex()


class LsmStorage:
    def __init__(self,
                 configuration: Configuration,
                 directory: str,
                 state: LsmState,
                 manifest: Manifest
                 ):
        self.directory = directory
        self._create_directory()
        self.manifest = manifest

        # Configuration
        self._configuration = configuration

        # State
        self.state = state

        # Concurrency handling
        self._locks = LsmLocks()

    def close(self) -> None:
        if self.state.memtable.approximate_size > 0:
            self._freeze()

        while len(self.state.immutable_memtables):
            self._trigger_flush()

    @classmethod
    def create(cls,
               max_sstable_size: Optional[int] = 262_144_000,
               block_size: Optional[int] = 65_536,
               levels_ratio: float = 0.1,
               max_l0_sstables: int = 10,
               nb_levels: int = 6,
               directory: Optional[str] = ".",
               ) -> "LsmStorage":

        configuration = Configuration(
            nb_levels=nb_levels,
            levels_ratio=levels_ratio,
            max_l0_sstables=max_l0_sstables,
            max_sstable_size=max_sstable_size,
            block_size=block_size,
        )

        state = LsmState(
            memtable=MemTable.create(directory=directory),
            immutable_memtables=deque(),
            sstables_level0=deque(),
            sstables_levels=[deque() for _ in range(nb_levels)],
        )

        return cls(
            directory=directory,
            configuration=configuration,
            state=state,
            manifest=Manifest.create(path=f"{directory}/manifest.txt", configuration=configuration)
        )

    def _try_freeze(self) -> None:
        """Checks if the memtable should be frozen or not.
        The memtable should be frozen if it is bigger than the `self._configuration.max_sstable_size` threshold.

        Further explanations on the details of this method:
        - It acquires the `self._locks.state` to ensure that only one freeze operation occurs at any given time
          (necessary because concurrency is allowed => two concurrent insert operations could end up doing a freeze)
        - The size of the memtable is checked twice: once before acquiring the `self._locks.state` and a second time
          after acquiring it. This is to avoid taking a lock unnecessarily the first time and thus to avoid impacting
          performance (preventing other operations needing this lock when it is not necessary). Once the condition is
          true and the `self._locks.state` is acquired, it is important to check the condition a second time because the
          memtable might have been frozen by another operation while this one was checking the condition and acquiring
          the lock.
        - It acquires a read lock on the state before reading the memtable's size. This is in order to comply to the
          concurrency protocol here: the mutex `self._locks.state` is for freeze operations (and other operations
          modifying the state), the ReadWriteLock `self._locks.read_write` is for reading/writing on one element.

        Note: another approach would have been to do the following:
        ```
        if approximate_size >= self._configuration.max_sstable_size:
            with self._locks.state:
                state_read_lock = self._locks.read_write.read()
                state_read_lock.__enter__()
                latest_approximate_size = self.state.memtable.approximate_size
                if latest_approximate_size >= self._configuration.max_sstable_size:
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
        with self._locks.read_write.read():
            approximate_size = self.state.memtable.approximate_size

        # Freeze should _not_ be triggered if the memtable is not full
        if approximate_size < self._configuration.max_sstable_size:
            return

        with self._locks.state:
            with self._locks.read_write.read():
                # Approximate size is re-read in case the memtable has already been flushed while waiting to acquire the
                # `self._locks.state` lock
                latest_approximate_size = self.state.memtable.approximate_size

            # Freeze should still _not_ be triggered if the memtable is not full
            if latest_approximate_size < self._configuration.max_sstable_size:
                return

            self._freeze()

    def _freeze(self) -> None:
        """Performs the freeze operation.
        The freeze operation consists in:
        - Creating a new empty memtable and adding it to the state as the current one;
        - Inserting the previous memtable in the list of immutable memtables.
        """
        # Create empty memtable
        new_memtable = MemTable.create(directory=self.directory)

        # Update state to add current memtable to immutable memtables and use the new empty memtable to write to
        with self._locks.read_write.write():
            self.state.immutable_memtables.insert(0, self.state.memtable)
            self.state.memtable = new_memtable

    def put(self, key: Record.Key, value: Record.Value) -> None:
        self.state.memtable.put(key=key, value=value)
        self._try_freeze()

    @staticmethod
    def _search_memtables(key: Record.Key, memtables: list[MemTable]) -> Optional[Record.Value]:
        for memtable in memtables:
            value = memtable.get(key=key)
            if value is not None:
                return value
        return None

    @staticmethod
    def _search_ss_tables(key: Record.Key, ss_tables: Deque[SSTable]) -> Optional[Record.Value]:
        for sstable in ss_tables:
            if not sstable.bloom_filter.may_contain(key=key):
                continue
            value = sstable.get(key=key)
            if value is not None:
                return value
        return None

    def get(self, key: Record.Key) -> Optional[Record.Value]:
        value = self._search_memtables(key=key, memtables=[self.state.memtable, *self.state.immutable_memtables])
        if value is not None:
            return value

        for level_ss_tables in [self.state.sstables_level0, *self.state.sstables_levels]:
            value = self._search_ss_tables(key=key, ss_tables=level_ss_tables)
            if value is not None:
                return value

        return None

    def scan(self, lower: Record.Key, upper: Record.Key) -> Iterator[Record]:
        active_memtable_iterator = self.state.memtable.scan(lower=lower, upper=upper)
        immutable_memtables_iterators = [memtable.scan(lower=lower, upper=upper) for memtable in
                                         self.state.immutable_memtables]
        sstables_iterators = [sstable.scan(lower=lower, upper=upper) for sstable in self.state.sstables_level0]

        iterator = MergingIterator(
            iterators=[active_memtable_iterator] + immutable_memtables_iterators + sstables_iterators)
        yield from iterator

    def _flush(self) -> None:
        """Performs the flush operation.
        The flush operation consists in:
        - Identifying the oldest immutable memtable;
        - Creating a new SSTable and filling it with the records in the selected memtable;
        - Updating the state to add the new SSTable at level 0 and remove the old memtable.

        In order to allow restarts and crash recoveries, a FlushEvent is recorded in the manifest and the WAL
        associated to the old memtable is deleted.
        """

        # Read the oldest memtable
        with self._locks.read_write.read():
            memtable_to_flush = self.state.immutable_memtables[-1]

        # Flush it to SSTable
        path = self._compute_path()
        sstable_builder = SSTableBuilder(sstable_size=self._configuration.max_sstable_size,
                                         block_size=self._configuration.block_size)
        memtable_iterator = MemTableIterator(memtable=memtable_to_flush)
        for record in memtable_iterator:
            sstable_builder.add(record=record)
        sstable = sstable_builder.build(path=path)

        # Update state to remove oldest memtable and add new SSTable
        with self._locks.read_write.write():
            flushed_memtable = self.state.immutable_memtables.pop()
            self.state.sstables_level0.insert(0, sstable)

        # Write to manifest
        event = FlushEvent(sstable_path=sstable.file.path)
        self.manifest.add_event(event=event)

        # Delete the WAL
        flushed_memtable.wal.remove_self()

    def _trigger_flush(self) -> None:
        """Triggers the flush operation and subsequent operations.
        This method's responsibility is only to organize the locking logic around operations.
        """
        with self._locks.state:
            self._flush()

        self._try_compact()

    def _compute_path(self) -> str:
        timestamp_in_us = int(time.time() * 1_000_000)
        return f"{self.directory}/{timestamp_in_us}.sst"

    def _create_directory(self) -> None:
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def _compute_compacted_ss_tables(self, records_iterator: BaseIterator) -> list[SSTable]:
        """Computes the new set of compacted SSTable by iterating over all records.
        Each SSTable of the new set should not exceed the maximum SSTable size.
        """
        compacted_ss_tables = []
        sstable_builder = SSTableBuilder(sstable_size=self._configuration.max_sstable_size,
                                         block_size=self._configuration.block_size)

        for record in records_iterator:
            sstable_builder.add(record=record)

            # Build the sstable when it exceeds the maximum size and instantiate a new builder
            if sstable_builder.current_buffer_position >= self._configuration.max_sstable_size:
                self._finalize_sstable(sstable_builder=sstable_builder, sstables=compacted_ss_tables)
                sstable_builder = SSTableBuilder(sstable_size=self._configuration.max_sstable_size,
                                                 block_size=self._configuration.block_size)

        # Build the last SSTable if it is not empty
        if sstable_builder.current_buffer_position > 0:
            self._finalize_sstable(sstable_builder=sstable_builder, sstables=compacted_ss_tables)

        return compacted_ss_tables

    def _finalize_sstable(self, sstable_builder: SSTableBuilder, sstables: list[SSTable]):
        sstable = sstable_builder.build(path=self._compute_path())
        sstables.append(sstable)

    def _compact(self,
                 input_sstables: Deque[SSTable],
                 output_sstables: Deque[SSTable],
                 input_level: int,
                 iterator_class: Type[MergingIterator] or Type[ConcatenatingIterator]) -> None:
        """Performs the compaction operation.
        Compaction consists in:
        - Identifying all SSTables that should be compacted
        - Compacting them into a new set of SSTables
        - Updating the state to remove the old set and add the new one to the list of tracked SSTables.

        In order to allow restarts and crash recoveries, a CompactionEvent is recorded in the manifest.
        """

        # Create records iterator from input SSTables
        with self._locks.read_write.read():
            sstables_to_compact = [sstable for sstable in input_sstables]
            records_iterator = iterator_class(iterators=[
                SSTableIterator(sstable=sstable) for sstable in sstables_to_compact
            ])

        # Compute compacted SSTables
        new_ss_tables = self._compute_compacted_ss_tables(records_iterator=records_iterator)

        # Update state to remove input SSTables and add new output SSTables
        with self._locks.state:
            with self._locks.read_write.write():
                output_sstables.extendleft(reversed(new_ss_tables))
                for sstable in sstables_to_compact:
                    input_sstables.remove(sstable)

        # Write to manifest
        event = CompactionEvent(
            input_sstables_paths=[sstable.file.path for sstable in sstables_to_compact],
            output_sstables_paths=[sstable.file.path for sstable in new_ss_tables],
            level=input_level)
        self.manifest.add_event(event=event)

        # Delete old SSTables
        for sstable in sstables_to_compact:
            sstable.file.remove_self()

    def _compact_l0(self) -> None:
        """Performs the compaction operation at level 0.
        """
        input_sstables = self.state.sstables_level0
        output_sstables = self.state.sstables_levels[0]

        self._compact(input_level=0,
                      input_sstables=input_sstables,
                      output_sstables=output_sstables,
                      iterator_class=MergingIterator)

    def _compact_l1_or_more(self, level: int) -> None:
        """Performs the compaction operation at level 1 or more.
        """
        output_level = min(level + 1, self._configuration.nb_levels)
        input_sstables = self.state.sstables_levels[level - 1]
        output_sstables = self.state.sstables_levels[output_level - 1]

        self._compact(input_level=level,
                      input_sstables=input_sstables,
                      output_sstables=output_sstables,
                      iterator_class=ConcatenatingIterator)

    def _try_compact(self) -> None:
        """Checks if a level should be compacted or not and compacts it if so.

        Compaction should be triggered:
        - at level 0 if the number of SSTables at level 0 exceeds a given threshold
          (`self._configuration.max_l0_sstables`)
        - at any other level if the ratio of the number of SSTables at this level over the number of SSTables at the
        next level exceeds a given threshold (`self._configuration.levels_ratio`).

        A compaction operation executed at a given level may trigger compaction at higher levels.
        In other words, compaction operations are triggered in cascade here.
        """

        # Try to compact level 0
        if len(self.state.sstables_level0) >= self._configuration.max_l0_sstables:
            self._compact_l0()

        # Try to compact other levels
        for level_index in range(self._configuration.nb_levels - 1):
            current_level = self.state.sstables_levels[level_index]
            next_level = self.state.sstables_levels[level_index + 1]
            if len(current_level) > 0 and len(current_level) >= self._configuration.levels_ratio * len(next_level):
                self._compact_l1_or_more(level=level_index + 1)

    @classmethod
    def reconstruct_from_manifest(cls, manifest_path: str) -> "LsmStorage":
        manifest = Manifest.build(manifest_path)
        ss_tables_levels = manifest.reconstruct_sstables()
        directory = os.path.dirname(manifest_path)

        state = LsmState(
            memtable=MemTable.create(directory=directory),
            immutable_memtables=deque(),
            sstables_level0=ss_tables_levels[0],
            sstables_levels=ss_tables_levels[1:]
        )

        return cls(
            configuration=manifest.configuration,
            directory=directory,
            state=state,
            manifest=manifest
        )
