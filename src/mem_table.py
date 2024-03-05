from src.record import Record
from src.red_black_tree import RedBlackTree


class MemTable:
    def __init__(self):
        self.id: int = 0
        self.map = RedBlackTree()
        self.approximate_size: int = 0

    def put(self, key: Record.Key, record: Record):
        self.map.insert(key=key, data=record.to_bytes())
        # Recomputing the approximate size of the mem table by adding the size of the record
        # This size is only approximate because, if a key is re-written or deleted, then the computed size will be
        # bigger than the actual one. Computing the exact size would imply some overhead to read first. That is why the
        # choice is to compute the _approximate size_.
        self.approximate_size += record.size

    def get(self, key: Record.Key):
        pass
