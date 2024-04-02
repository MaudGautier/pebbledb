from typing import Iterator

from src.blocks import DataBlock
from src.memtable import MemTable
from src.record import Record
from src.sstable import SSTable


class BaseIterator(Iterator):
    def __init__(self):
        pass

    def __iter__(self):
        raise NotImplementedError()

    def __next__(self):
        raise NotImplementedError()


class MemTableIterator(BaseIterator):
    def __init__(self, memtable: MemTable):
        super().__init__()
        self.generator = iter(memtable.map)
        self.current = None

    def __iter__(self) -> "MemTableIterator":
        return self

    def __next__(self) -> Record:
        self.current = next(self.generator, None)
        if self.current is None:
            raise StopIteration

        encoded_record = self.current
        return Record.from_bytes(data=encoded_record)


class DataBlockIterator(BaseIterator):
    def __init__(self, block: DataBlock):
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
    def __init__(self, sstable: SSTable):
        super().__init__()
        self._index = 0
        self.sstable = sstable
        self.block_iterator = DataBlockIterator(block=self._get_first_data_block())

    def _get_first_data_block(self) -> DataBlock:
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
