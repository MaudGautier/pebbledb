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


# event1 = CompactionEvent()
# events = [event1, FlushEvent(...), ]

# COMPACTION EVENT: input files, output files, output level
# FLUSH EVENT: output sstable

class Manifest:
    def __init__(self, events):  # TODO stop passing events - read them from file at some point
        self.events = events

    def reconstruct(self) -> LsmStorage:
        l0_sstables = []
        for event in self.events:
            if event.type == "Flush":
                l0_sstables.insert(0, event.sstable)

        store = LsmStorage()
        store.ss_tables = l0_sstables

        return store
