from heapq import heappush, heappop
from typing import Iterator, TYPE_CHECKING, Optional

from src.record import Record, MAX_SNAPSHOT
from src.red_black_tree import Node

# TODO: Should be possible to remove this when finished decoupling iterators logic from DataBlocks
if TYPE_CHECKING:
    from src.blocks import DataBlock
    from src.memtable import MemTable
    from src.sstable import SSTable


class BaseIterator(Iterator):
    def __init__(self):
        pass

    def __iter__(self):
        raise NotImplementedError()

    def __next__(self):
        raise NotImplementedError()


class MemTableIterator(BaseIterator):
    def __init__(self,
                 memtable: "MemTable",
                 snapshot: Optional[int] = None,
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None):
        super().__init__()
        self.generator = self._select_generator(memtable=memtable, start_key=start_key, end_key=end_key)
        self.current = None
        self.snapshot = snapshot if snapshot is not None else MAX_SNAPSHOT

    @staticmethod
    def _select_generator(
            memtable: "MemTable",
            start_key: Optional[Record.Key],
            end_key: Optional[Record.Key]) -> Iterator[Node.Data]:
        if start_key is None and end_key is None:
            return iter(memtable.map)

        if start_key is not None and end_key is not None:
            return iter(memtable.map.scan(lower=start_key, upper=end_key))

        raise ValueError(f"Only 'start_key' or 'end_key' was passed. The iterator cannot handle this case!")

    def __iter__(self) -> "MemTableIterator":
        return self

    def __next__(self) -> list[Record]:
        self.current = next(self.generator, None)

        if self.current is None:
            raise StopIteration

        selected_versions = [
            record_version
            # Note: Record versions are from oldest to most recent (hence the need to reverse)
            for encoded_record_version in reversed(self.current)
            # Keep only versions whose sequence number is smaller than the snapshot
            if (record_version := Record.from_bytes(data=encoded_record_version)).sequence_number <= self.snapshot
        ]

        return selected_versions


class FlushIterator(MemTableIterator):
    def __init__(self,
                 memtable: "MemTable",
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None):
        super().__init__(memtable=memtable, start_key=start_key, end_key=end_key)


class ScanMemtableIterator(MemTableIterator):
    def __init__(self,
                 memtable: "MemTable",
                 snapshot: Optional[int] = None,
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None,
                 ):
        super().__init__(memtable=memtable, start_key=start_key, end_key=end_key, snapshot=snapshot)

    def __next__(self) -> Record:
        records = super().__next__()
        return self._select_most_recent_record(records=records)

    @staticmethod
    def _select_most_recent_record(records: list[Record]) -> Record:
        most_recent_record: Optional[Record] = None

        for record in records:
            if most_recent_record is not None and most_recent_record.sequence_number >= record.sequence_number:
                continue

            most_recent_record = record

        return most_recent_record


class DataBlockIterator(BaseIterator):
    def __init__(self,
                 block: "DataBlock",
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None,
                 ):
        super().__init__()
        self.block = block
        self._index = self._select_index(key=start_key)
        self._end_key = end_key

    def _get_record(self, record_index: int):
        offsets = self.block.offsets + [len(self.block.data)]
        offset_start = offsets[record_index]
        offset_end = offsets[record_index + 1]
        encoded_record = self.block.data[offset_start:offset_end]
        record = Record.from_bytes(data=encoded_record)
        return record

    def _select_index(self, key: Optional[Record.Key] = None) -> int:
        """Selects the first key that is >= key"""
        if key is None:
            return 0

        low, high = 0, len(self.block.offsets)
        while low < high:
            mid = int(low + (high - low) / 2)
            record = self._get_record(record_index=mid)
            if record.key < key:
                low = mid + 1
            else:
                high = mid

        return low

    def __iter__(self) -> "DataBlockIterator":
        return self

    def __next__(self) -> Record:
        if self._index >= len(self.block.offsets):
            raise StopIteration()

        record = self._get_record(record_index=self._index)

        if self._end_key and record.key > self._end_key:
            raise StopIteration

        self._index += 1

        return record


class SSTableIterator(BaseIterator):
    def __init__(self,
                 sstable: "SSTable",
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None
                 ):
        super().__init__()
        self._index = 0
        self.sstable = sstable
        self.start_key = start_key
        self.end_key = end_key
        self.block_iterator = self._get_block_iterator(block_id=0)

    def _get_block_iterator(self, block_id: int):
        return DataBlockIterator(
            block=self.sstable.read_data_block(block_id=block_id),
            start_key=self.start_key,
            end_key=self.end_key)

    def __iter__(self) -> "SSTableIterator":
        return self

    def __next__(self) -> Record:
        try:
            return next(self.block_iterator)
        except StopIteration:
            self._index += 1
            if self._index >= len(self.sstable.meta_blocks):
                raise StopIteration()

            self.block_iterator = self._get_block_iterator(block_id=self._index)
            return next(self)


class CompactSSTableIterator(SSTableIterator):
    """Yields all records within a SSTable that:
    - Have a sequence number higher than the provided snapshot
    - Is the last (i.e. most recently written) version that is equal or below the snapshot

    Therefore, all versions below the snapshot (except for the latest) are ignored, and all others are yielded.
    """

    def __init__(self,
                 sstable: "SSTable",
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None,
                 snapshot: Optional[int] = None
                 ):
        super().__init__(sstable=sstable, start_key=start_key, end_key=end_key)
        self.snapshot = snapshot if snapshot else MAX_SNAPSHOT
        self.last_yielded_record_below_snapshot = None

    def should_skip(self, record: Record) -> bool:
        # Keep all versions above min snapshot
        if record.sequence_number > self.snapshot:
            return False

        # Keep one version below snapshot (=> skip all duplicates after the one yielded)
        if self.last_yielded_record_below_snapshot and record.is_duplicate(self.last_yielded_record_below_snapshot):
            return True

        # Remember who has been yielded and is below the snapshot
        self.last_yielded_record_below_snapshot = record

        return False

    def __next__(self) -> Record:
        try:
            while True:
                record = next(self.block_iterator)

                if self.should_skip(record):
                    continue

                # Otherwise return
                return record

        except StopIteration:
            self._index += 1
            if self._index >= len(self.sstable.meta_blocks):
                raise StopIteration()

            self.block_iterator = self._get_block_iterator(block_id=self._index)
            return next(self)


class ScanSSTableIterator(SSTableIterator):
    def __init__(self,
                 sstable: "SSTable",
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None,
                 snapshot: Optional[int] = None
                 ):
        super().__init__(sstable=sstable, start_key=start_key, end_key=end_key)
        self.last_seen_record = None
        self.snapshot = snapshot if snapshot else MAX_SNAPSHOT

    def __next__(self) -> Record:
        try:
            while True:
                record = next(self.block_iterator)

                # Skip record if duplicate of previous one
                if self.last_seen_record and record.is_duplicate(self.last_seen_record):
                    continue

                # Skip record if sequence number is above snapshot
                if record.sequence_number > self.snapshot:
                    continue

                # Otherwise return
                self.last_seen_record = record
                return record

        except StopIteration:
            self._index += 1
            if self._index >= len(self.sstable.meta_blocks):
                raise StopIteration()

            self.block_iterator = self._get_block_iterator(block_id=self._index)
            return next(self)


class MergingIterator(BaseIterator):
    def __init__(self, iterators: list[BaseIterator]):
        super().__init__()
        self.iterators = iterators
        self.merged_and_filtered_iterator = self._filter_duplicate_keys(self._merge_iterators())

    def __iter__(self) -> "MergingIterator":
        return self

    def __next__(self):
        return next(self.merged_and_filtered_iterator)

    def _merge_iterators(self) -> BaseIterator:
        no_item = object()
        heap = []

        def get_next(iterator: BaseIterator):
            try:
                return next(iterator)
            except StopIteration:
                return no_item

        def heap_key(record: Record, sequence_number: int):
            return record.key, sequence_number

        def try_push_iterator(iterator_index: int):
            next_item = get_next(iterator=self.iterators[iterator_index])
            if next_item is no_item:
                return
            heappush(heap, (heap_key(next_item, iterator_index), next_item))

        for i in range(len(self.iterators)):
            try_push_iterator(i)

        while len(heap):
            (_, i), value = heappop(heap)
            yield value
            try_push_iterator(i)

    @staticmethod
    def _filter_duplicate_keys(iterator: Iterator[Record]) -> Iterator[Record]:
        previous_item = None
        for item in iterator:
            if previous_item is not None and item.is_duplicate(previous_item):
                continue
            yield item
            previous_item = item


class ConcatenatingIterator(BaseIterator):
    def __init__(self, iterators: list[BaseIterator]):
        super().__init__()
        self.iterators = iterators
        self.iterator = self._concatenate_iterators()

    def __iter__(self) -> "ConcatenatingIterator":
        return self

    def __next__(self):
        return next(self.iterator)

    def _concatenate_iterators(self) -> BaseIterator:
        for iterator in self.iterators:
            yield from iterator
