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
    def __init__(self, block: "DataBlock"):
        super().__init__()
        self._index = 0
        self.block = block

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
        return Record.from_bytes(data=encoded_record)


class SSTableIterator(BaseIterator):
    def __init__(self, sstable: "SSTable"):
        super().__init__()
        self._index = 0
        self.sstable = sstable
        self.block_iterator = DataBlockIterator(block=self._get_first_data_block())

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

            self.block_iterator = DataBlockIterator(block=self.sstable.read_data_block(block_id=self._index))
            return next(self)
