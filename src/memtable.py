from typing import Optional, Iterator

from src.iterators import MemTableIterator
from src.record import Record
from src.red_black_tree import RedBlackTree


class MemTable:
    def __init__(self, map, approximate_size, directory):
        self.map = map
        self.approximate_size: int = approximate_size
        self.directory = directory

    @classmethod
    def create(cls, directory: str):
        # wal = self._create_wal()
        return cls(map=RedBlackTree(), directory=directory, approximate_size=0)  # TODO pass wal

    def scan(self, lower: Record.Key, upper: Record.Key) -> MemTableIterator:
        return MemTableIterator(memtable=self, start_key=lower, end_key=upper)

    def put(self, key: Record.Key, value: Record.Value):
        record = Record(key=key, value=value)
        self.map.insert(key=record.key, data=record.to_bytes())
        # Recomputing the approximate size of the mem table by adding the size of the record
        # This size is only approximate because, if a key is re-written or deleted, then the computed size will be
        # bigger than the actual one. Computing the exact size would imply some overhead to read first. That is why the
        # choice is to compute the _approximate size_.
        self.approximate_size += record.size

    def get(self, key: Record.Key) -> Optional[Record.Value]:
        encoded_record = self.map.get(key=key)
        if encoded_record is None:
            return None
        decoded_record = Record.from_bytes(encoded_record)
        return decoded_record.value
