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
    def __init__(self, events):  # TODO stop passing events - read them from file at some point
        self.events = events

    def reconstruct(self) -> LsmStorage:
        l0_sstables = deque()
        l1_sstables = deque()
        l2_sstables = deque()
        for event in self.events:
            if event.type == "Flush":
                l0_sstables.insert(0, event.sstable)
            if event.type == "Compaction":
                if event.output_level == 1:
                    for sstable in event.output_sstables:
                        l1_sstables.insert(0, sstable)
                    for sstable in event.input_sstables:
                        l0_sstables.remove(sstable)
                if event.output_level == 2:
                    for sstable in event.output_sstables:
                        l2_sstables.insert(0, sstable)
                    for sstable in event.input_sstables:
                        l1_sstables.remove(sstable)

        store = LsmStorage()
        store.ss_tables = l0_sstables
        store.ss_tables_levels.append(l1_sstables)  # TODO: do this better later
        store.ss_tables_levels.append(l2_sstables)

        return store
