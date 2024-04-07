from heapq import heappush, heappop
from typing import Iterator, TYPE_CHECKING, Optional

from src.record import Record

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
                 start_key: Optional[Record.Key] = None,
                 end_key: Optional[Record.Key] = None):
        super().__init__()
        self.generator = self._select_generator(memtable=memtable, start_key=start_key, end_key=end_key)
        self.current = None

    @staticmethod
    def _select_generator(
            memtable: "MemTable",
            start_key: Optional[Record.Key],
            end_key: Optional[Record.Key]) -> Iterator[bytes]:
        if start_key is None and end_key is None:
            return iter(memtable.map)

        if start_key is not None and end_key is not None:
            return iter(memtable.map.scan(lower=start_key, upper=end_key))

        raise ValueError(f"Only 'start_key' or 'end_key' was passed. The iterator cannot handle this case!")

    def __iter__(self) -> "MemTableIterator":
        return self

    def __next__(self) -> Record:
        self.current = next(self.generator, None)
        if self.current is None:
            raise StopIteration

        encoded_record = self.current
        return Record.from_bytes(data=encoded_record)


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

    def _select_index(self, key: Optional[Record.Key] = None) -> int:
        """Selects the first key that is >= key"""
        if key is None:
            return 0

        offsets = self.block.offsets + [len(self.block.data)]

        low, high = 0, len(self.block.offsets)
        while low < high:
            mid = int(low + (high - low) / 2)
            offset_start = offsets[mid]
            offset_end = offsets[mid + 1]
            encoded_record = self.block.data[offset_start:offset_end]
            record = Record.from_bytes(data=encoded_record)
            if record.key == key:
                return mid
            if record.key < key:
                low = mid + 1
            if record.key > key:
                high = mid

        # TODO mettre tout ca dans une autre fonction et StopIteration si plus que le truc (ça devrait être bon avec ce que j'ai - ajouter un test)
        return low

    def __iter__(self) -> "DataBlockIterator":
        return self

    def __next__(self) -> Record:
        if self._index >= len(self.block.offsets):
            raise StopIteration()
        offset = self.block.offsets[self._index]
        next_offset = self.block.offsets[self._index + 1] if self._index + 1 < len(self.block.offsets) else len(
            self.block.data)
        self._index += 1
        encoded_record = self.block.data[offset:next_offset]
        record = Record.from_bytes(data=encoded_record)
        if self._end_key and record.key > self._end_key:
            raise StopIteration
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
        self.block_iterator = DataBlockIterator(block=self._get_first_data_block(), start_key=start_key,
                                                end_key=end_key)
        self.start_key = start_key
        self.end_key = end_key

    def _get_first_data_block(self) -> "DataBlock":
        return self.sstable.read_data_block(block_id=0)

    def __iter__(self) -> "SSTableIterator":
        return self

    def __next__(self) -> Record:
        try:
            return next(self.block_iterator)
        except StopIteration:
            self._index += 1
            if self._index >= len(self.sstable.meta_blocks):
                raise StopIteration()

            self.block_iterator = DataBlockIterator(block=self.sstable.read_data_block(block_id=self._index),
                                                    start_key=self.start_key,
                                                    end_key=self.end_key)
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
