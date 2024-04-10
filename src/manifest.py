from collections import deque

from src.lsm_storage import LsmStorage
from src.sstable import SSTable


class Event:
    def __init__(self):
        pass


class CompactionEvent(Event):
    def __init__(self, input_sstables: list[SSTable], output_sstables: list[SSTable], level: int):
        super().__init__()
        self.input_sstables = input_sstables
        self.output_sstables = output_sstables
        self.level = level


class FlushEvent(Event):
    def __init__(self, sstable: SSTable):
        super().__init__()
        self.sstable = sstable


class Manifest:
    def __init__(self, events, nb_levels):  # TODO stop passing events and nb_levels - read them from file at some point
        self.events = events
        self.nb_levels = nb_levels

    def reconstruct(self) -> LsmStorage:
        ss_tables_levels = [deque() for _ in range(self.nb_levels + 1)]

        for event in self.events:
            if isinstance(event, FlushEvent):
                ss_tables_levels[0].insert(0, event.sstable)
            if isinstance(event, CompactionEvent):
                level = event.level
                for sstable in event.output_sstables:
                    ss_tables_levels[level + 1].insert(0, sstable)
                for sstable in event.input_sstables:
                    ss_tables_levels[level].remove(sstable)

        store = LsmStorage()
        store.ss_tables = ss_tables_levels[0]
        store.ss_tables_levels = ss_tables_levels[1:]

        return store
