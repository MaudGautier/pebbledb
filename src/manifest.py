import struct
from collections import deque

from src.lsm_storage import LsmStorage
from src.sstable import SSTable


class Event:
    def __init__(self):
        pass


class FlushEvent(Event):
    def __init__(self, sstable: SSTable):
        super().__init__()
        self.sstable = sstable

    def __eq__(self, other):
        if not isinstance(other, FlushEvent):
            return NotImplemented
        return self.sstable == other.sstable


class CompactionEvent(Event):
    def __init__(self, input_sstables: list[SSTable], output_sstables: list[SSTable], level: int):
        super().__init__()
        self.input_sstables = input_sstables
        self.output_sstables = output_sstables
        self.level = level


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


class ManifestRecord:
    def __init__(self, event: Event):
        self.event = event
        self.category_encoding = {"FLUSH": 0, "COMPACT": 1}

    def to_bytes(self) -> bytes:
        raise NotImplementedError()

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestRecord":
        raise NotImplementedError()


class ManifestFlushRecord(ManifestRecord):
    """This class handles encoding and decoding of ManifestFlushRecords.

    Each ManifestFlushRecord has the following format:
    +--------+-----------+-----------------+
    | FLUSH  | path_size |      path       | # TODO: ADD TIMESTAMP ???
    +--------+-----------+-----------------+
    | 1 byte |  1 byte   | path_size bytes |
    +--------+-----------+-----------------+
    """

    def __init__(self, event: FlushEvent):
        super().__init__(event=event)
        self.event = event  # TODO: check if there is a way _NOT_ to do that
        self.category = self.category_encoding["FLUSH"]

    def to_bytes(self) -> bytes:
        sstable_path = self.event.sstable.file.path
        encoded_path_size = struct.pack("B", len(sstable_path))
        encoded_path = sstable_path.encode(encoding="utf-8")
        encoded_category = struct.pack("B", self.category)

        return encoded_category + encoded_path_size + encoded_path

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestFlushRecord":
        category = struct.unpack("B", data[:1])[0]
        filename_size = struct.unpack("B", data[1:2])[0]
        file_path = data[2:2 + filename_size].decode("utf-8")

        # TODO: move category encoding in ManifestRecord instead (parent class) and then dispatch to children classes
        if category != 0:  # FLUSH
            raise ValueError("PROBLEM NOT HANDLED!!!")

        sstable = SSTable.build_from_path(path=file_path)
        event = FlushEvent(sstable=sstable)

        return cls(event=event)
