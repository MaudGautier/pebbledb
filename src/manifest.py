from collections import deque

from src.lsm_storage import LsmStorage
from src.sstable import SSTable


class Event:
    def __init__(self):
        pass


class CompactionEvent(Event):
    def __init__(self, input_sstables: list[SSTable], output_sstables: list[SSTable], output_level: int):
        super().__init__()
        self.type = "Compaction"
        self.input_sstables = input_sstables
        self.output_sstables = output_sstables
        self.output_level = output_level


class FlushEvent(Event):
    def __init__(self, sstable: SSTable):
        super().__init__()
        self.type = "Flush"
        self.sstable = sstable


class Manifest:
    def __init__(self, events, nb_levels):  # TODO stop passing events and nb_levels - read them from file at some point
        self.events = events
        self.nb_levels = nb_levels

    def reconstruct(self) -> LsmStorage:
        ss_tables_levels = [deque() for i in range(self.nb_levels)]

        l0_sstables = deque()
        for event in self.events:
            if event.type == "Flush":
                l0_sstables.insert(0, event.sstable)
            if event.type == "Compaction":
                level = event.output_level - 1
                if level == 0:
                    for sstable in event.output_sstables:
                        ss_tables_levels[level].insert(0, sstable)
                    for sstable in event.input_sstables:
                        l0_sstables.remove(sstable)
                if level >= 1:
                    for sstable in event.output_sstables:
                        ss_tables_levels[level].insert(0, sstable)
                    for sstable in event.input_sstables:
                        ss_tables_levels[level - 1].remove(sstable)

        store = LsmStorage()
        store.ss_tables = l0_sstables
        store.ss_tables_levels = ss_tables_levels

        return store
