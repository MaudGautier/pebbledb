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
    +--------+-----------------------+-----------------------------+
    | FLUSH  | manifest_sstable_size |       ManifestSSTable       | # TODO: ADD TIMESTAMP ???
    +--------+-----------------------+-----------------------------+
    | 1 byte |        1 byte         | manifest_sstable_size bytes |
    +--------+-----------------------+-----------------------------+
    """

    def __init__(self, event: FlushEvent):
        super().__init__(event=event)
        self.event = event  # TODO: check if there is a way _NOT_ to do that
        self.category = self.category_encoding["FLUSH"]

    def to_bytes(self) -> bytes:
        encoded_category = struct.pack("B", self.category)
        manifest_ss_table = ManifestSSTable(sstable=self.event.sstable)
        encoded_manifest_sstable = manifest_ss_table.to_bytes()
        encoded_size = struct.pack("B", len(encoded_manifest_sstable))

        return encoded_category + encoded_size + encoded_manifest_sstable

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestFlushRecord":
        category = struct.unpack("B", data[:1])[0]
        # TODO: move category encoding in ManifestRecord instead (parent class) and then dispatch to children classes
        if category != 0:  # FLUSH
            raise ValueError("PROBLEM NOT HANDLED!!!")

        size = struct.unpack("B", data[1:2])[0]
        sstable = ManifestSSTable.from_bytes(data=data[2:2 + size]).sstable
        event = FlushEvent(sstable=sstable)

        return cls(event=event)


class ManifestSSTable:
    """This class handles encoding and decoding of ManifestSSTables.

    Each ManifestSSTable has the following format:
    +-----------+-----------------+
    | path_size |      path       |
    +-----------+-----------------+
    |  1 byte   | path_size bytes |
    +-----------+-----------------+
    """

    def __init__(self, sstable: SSTable):
        self.sstable = sstable

    @property
    def size(self):
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        sstable_path = self.sstable.file.path
        encoded_path_size = struct.pack("B", len(sstable_path))
        encoded_path = sstable_path.encode(encoding="utf-8")

        return encoded_path_size + encoded_path

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestSSTable":
        filename_size = struct.unpack("B", data[0:1])[0]
        file_path = data[1:1 + filename_size].decode("utf-8")

        sstable = SSTable.build_from_path(path=file_path)

        return cls(sstable=sstable)


class ManifestCompactionRecord(ManifestRecord):
    """This class handles encoding and decoding of ManifestCompactionRecords.

    Each ManifestCompactionRecord has the following format:
    +----------+---------------------------------+-----------------------+-----------------------+
    | Category |              Extra              |     Input SSTables    |    Output SSTables    |
    +----------+---------------------------------+-----------------------+-----------------------+
    | COMPACT  | level | iSSTs_size | oSSTs_size | iSST_1 | ... | iSST_n | oSST_1 | ... | oSST_n |
    +----------+---------------------------------+-----------------------+-----------------------+
    (iSST_k = input SSTable k, oSST_k = output SSTable k - encoded as ManifestSSTable)
    (Level is encoded with 1 byte)

    With the Extra section having the following format:
    +--------+------------+------------+
    | level  | iSSTs_size | oSSTs_size |
    +--------+------------+------------+
    | 1 byte |  2 bytes   |  2 bytes   |
    +--------+------------+------------+
    """

    def __init__(self, event: CompactionEvent):
        super().__init__(event=event)
        self.event = event
        self.category = self.category_encoding["COMPACT"]

    def to_bytes(self) -> bytes:
        encoded_category = struct.pack("B", self.category)
        encoded_level = struct.pack("B", self.event.level)

        encoded_in_sstables = self.encode_manifest_sstables_block(sstables=self.event.input_sstables)
        encoded_out_sstables = self.encode_manifest_sstables_block(sstables=self.event.output_sstables)
        encoded_size_in_sstables = struct.pack("H", len(encoded_in_sstables))
        encoded_size_out_sstables = struct.pack("H", len(encoded_out_sstables))

        return encoded_category + encoded_level + encoded_size_in_sstables + encoded_size_out_sstables + encoded_in_sstables + encoded_out_sstables

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestCompactionRecord":
        category_start, extra_start = 0, 1
        category = struct.unpack("B", data[category_start:extra_start])[0]
        # TODO: move category encoding in ManifestRecord instead (parent class) and then dispatch to children classes
        if category != 1:  # COMPACT
            raise ValueError("PROBLEM NOT HANDLED!!!")

        level = struct.unpack("B", data[extra_start:extra_start + 1])[0]
        size_in_sstables = struct.unpack("H", data[extra_start + 1: extra_start + 3])[0]
        size_out_sstables = struct.unpack("H", data[extra_start + 3: extra_start + 5])[0]

        input_sstables_start = extra_start + 5
        output_sstables_start = input_sstables_start + size_in_sstables
        output_sstables_end = output_sstables_start + size_out_sstables
        encoded_input_sstables = data[input_sstables_start:output_sstables_start]
        encoded_output_sstables = data[output_sstables_start:output_sstables_end]

        decoded_input_sstables = cls.decode_manifest_sstables_block(data=encoded_input_sstables)
        decoded_output_sstables = cls.decode_manifest_sstables_block(data=encoded_output_sstables)

        compaction_event = CompactionEvent(input_sstables=decoded_input_sstables,
                                           output_sstables=decoded_output_sstables,
                                           level=level)

        return cls(event=compaction_event)

    @staticmethod
    def decode_manifest_sstables_block(data: bytes) -> list[SSTable]:
        sstables = []

        while len(data):
            manifest_sstable = ManifestSSTable.from_bytes(data=data)
            sstables.append(manifest_sstable.sstable)
            data = data[manifest_sstable.size:]

        return sstables

    @staticmethod
    def encode_manifest_sstables_block(sstables: list[SSTable]) -> bytes:
        return b''.join([ManifestSSTable(sstable).to_bytes() for sstable in sstables])
