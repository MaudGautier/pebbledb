import os.path
import time
from typing import Optional

from src.iterators import MemTableIterator
from src.record import Record
from src.red_black_tree import RedBlackTree
from src.wal import WriteAheadLog


class MemTable:
    def __init__(self, map: RedBlackTree, approximate_size: int, directory: str, wal: WriteAheadLog):
        self.map = map
        self.approximate_size: int = approximate_size
        self.directory = directory
        self.wal = wal

    @classmethod
    def create(cls, directory: str):
        wal = cls._create_wal(directory=directory)
        return cls(map=RedBlackTree(), directory=directory, approximate_size=0, wal=wal)

    @classmethod
    def recover_from_wal(cls, wal_path: str):
        red_black_tree = RedBlackTree()
        with open(wal_path, "rb") as f:
            data = f.read()

        all_records = Record.list_from_bytes(data=data)
        approximate_size = 0
        for record in all_records:
            red_black_tree.insert(key=record.key, data=record.to_bytes())
            approximate_size += record.size

        directory = os.path.dirname(wal_path)
        wal = WriteAheadLog(path=wal_path)

        return cls(directory=directory, approximate_size=approximate_size, map=red_black_tree, wal=wal)

    @staticmethod
    def _create_wal(directory) -> WriteAheadLog:
        timestamp_in_us = time.time()

        return WriteAheadLog(path=f"{directory}/{timestamp_in_us}.wal")

    def scan(self, lower: Record.Key, upper: Record.Key) -> MemTableIterator:
        return MemTableIterator(memtable=self, start_key=lower, end_key=upper)

    def put(self, key: Record.Key, value: Record.Value):
        record = Record(key=key, value=value)
        self.wal.insert(record=record)
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
