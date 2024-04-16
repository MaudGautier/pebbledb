import os
import struct
from collections import deque
from typing import Dict, Type, BinaryIO

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

    def __eq__(self, other):
        if not isinstance(other, CompactionEvent):
            return NotImplemented
        return (self.input_sstables == other.input_sstables
                and self.output_sstables == other.output_sstables
                and self.level == other.level)


class ManifestHeader:
    def __init__(
            self,
            nb_levels: int,
            levels_ratio: float,
            max_l0_sstables: int,
            max_sstable_size: int,
            block_size: int):
        self.nb_levels = nb_levels
        self.levels_ratio = levels_ratio
        self.max_l0_sstables = max_l0_sstables
        self.max_sstable_size = max_sstable_size
        self.block_size = block_size

    def __eq__(self, other) -> bool:
        if not isinstance(other, ManifestHeader):
            return NotImplemented
        return (
                self.nb_levels == other.nb_levels and
                self.levels_ratio == other.levels_ratio and
                self.max_l0_sstables == other.max_l0_sstables and
                self.max_sstable_size == other.max_sstable_size and
                self.block_size == other.block_size
        )

    @property
    def size(self) -> int:
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        encoded_nb_levels = struct.pack("i", self.nb_levels)
        encoded_levels_ratio = struct.pack("d", self.levels_ratio)
        encoded_max_l0_sstables = struct.pack("i", self.max_l0_sstables)
        encoded_max_sstable_size = struct.pack("i", self.max_sstable_size)
        encoded_block_size = struct.pack("i", self.block_size)

        return (
                encoded_nb_levels +
                encoded_levels_ratio +
                encoded_max_l0_sstables +
                encoded_max_sstable_size +
                encoded_block_size)

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestHeader":
        decoded_nb_levels = struct.unpack("i", data[:4])[0]
        decoded_levels_ratio = struct.unpack("d", data[4:12])[0]
        decoded_max_l0_sstables = struct.unpack("i", data[12:16])[0]
        decoded_max_sstable_size = struct.unpack("i", data[16:20])[0]
        decoded_block_size = struct.unpack("i", data[20:24])[0]

        return cls(nb_levels=decoded_nb_levels, levels_ratio=decoded_levels_ratio,
                   max_l0_sstables=decoded_max_l0_sstables, max_sstable_size=decoded_max_sstable_size,
                   block_size=decoded_block_size)


class ManifestFile:
    def __init__(self, file: BinaryIO, path: str):
        self.file = file
        self.path = path

    @staticmethod
    def _file_exists(path) -> bool:
        return os.path.isfile(path)

    @classmethod
    def create(cls,
               path: str,
               nb_levels: int,
               levels_ratio: float,
               max_l0_sstables: int,
               max_sstable_size: int,
               block_size: int):

        if cls._file_exists(path=path):
            raise ValueError(f"Cannot create the file because there is already one at {path}")

        encoded_header = ManifestHeader(nb_levels=nb_levels,
                                        levels_ratio=levels_ratio,
                                        max_l0_sstables=max_l0_sstables,
                                        max_sstable_size=max_sstable_size,
                                        block_size=block_size).to_bytes()
        file = open(path, "ab")
        file.write(encoded_header)
        file.flush()

        return cls(file=file, path=path)

    @classmethod
    def open(cls, path: str):
        if not cls._file_exists(path=path):
            raise ValueError(f"Cannot open the file because there is none at {path}")

        file = open(path, "ab")

        return cls(file=file, path=path)

    def write_event(self, event: Event):
        record = ManifestRecord(event=event)
        encoded_record = record.to_bytes()
        self.file.write(encoded_record)
        self.file.flush()

    def decode(self):
        with open(self.path, "rb") as f:
            data = f.read()

        # Decode header
        header = ManifestHeader.from_bytes(data=data)
        checkpoint = header.size

        # Decode events
        events = self.decode_events(data=data[checkpoint:])

        return header, events

    @staticmethod
    def decode_events(data: bytes) -> list[Event]:
        events = []
        while len(data):
            manifest_record = ManifestRecord.from_bytes(data=data)
            event, checkpoint = manifest_record.event, manifest_record.size
            events.append(event)
            data = data[checkpoint:]
        return events


class Manifest:
    def __init__(self, file, events=None, nb_levels=None):
        self.file = file
        # TODO stop passing events and nb_levels - read them from file at some point
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
    category_encoding: Dict[Type[Event], int] = {FlushEvent: 0, CompactionEvent: 1}
    category_decoding = {0: 'ManifestFlushRecord', 1: 'ManifestCompactionRecord'}

    def __init__(self, event: Event):
        self.event = event

    @property
    def size(self) -> int:
        return len(self.to_bytes())

    @property
    def event_type(self):
        event_type = type(self.event)
        if event_type not in self.category_encoding.keys():
            raise ValueError(f"Events of type {event_type} are not handled")

        return event_type

    @property
    def category(self) -> int:
        return self.category_encoding[self.event_type]

    @classmethod
    def get_record_class(cls, category: int):
        if category not in cls.category_decoding.keys():
            raise ValueError(f"Category {category} does not map to any known record class. "
                             f"Possible categories are {cls.category_decoding.keys()}.")
        return globals()[cls.category_decoding[category]]

    def to_bytes(self) -> bytes:
        category_byte = struct.pack("B", self.category)
        record_class = self.get_record_class(category=self.category)

        return category_byte + record_class(event=self.event).to_bytes()

    @classmethod
    def from_bytes(cls, data: bytes):
        category = struct.unpack("B", data[:1])[0]
        record_class = cls.get_record_class(category=category)
        event = record_class.from_bytes(data[1:]).event

        return cls(event=event)


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
        super().__init__(event)
        self.event = event

    def to_bytes(self):
        manifest_ss_table = ManifestSSTable(sstable=self.event.sstable)
        encoded_manifest_sstable = manifest_ss_table.to_bytes()
        encoded_size = struct.pack("B", len(encoded_manifest_sstable))

        return encoded_size + encoded_manifest_sstable

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestFlushRecord":
        size = struct.unpack("B", data[0:1])[0]
        sstable = ManifestSSTable.from_bytes(data=data[1:1 + size]).sstable
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


class ManifestSSTablesBlock:
    def __init__(self, sstables: list[SSTable]):
        self.sstables = sstables

    def to_bytes(self) -> bytes:
        return b''.join([ManifestSSTable(sstable).to_bytes() for sstable in self.sstables])

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestSSTablesBlock":
        sstables = []

        while len(data):
            manifest_sstable = ManifestSSTable.from_bytes(data=data)
            sstables.append(manifest_sstable.sstable)
            data = data[manifest_sstable.size:]

        return cls(sstables=sstables)


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
        super().__init__(event)
        self.event = event

    def to_bytes(self) -> bytes:
        encoded_level = struct.pack("B", self.event.level)

        encoded_in_sstables = ManifestSSTablesBlock(sstables=self.event.input_sstables).to_bytes()
        encoded_out_sstables = ManifestSSTablesBlock(sstables=self.event.output_sstables).to_bytes()
        encoded_size_in_sstables = struct.pack("H", len(encoded_in_sstables))
        encoded_size_out_sstables = struct.pack("H", len(encoded_out_sstables))

        return (encoded_level + encoded_size_in_sstables + encoded_size_out_sstables +
                encoded_in_sstables + encoded_out_sstables)

    @classmethod
    def from_bytes(cls, data: bytes) -> "ManifestCompactionRecord":
        extra_start = 0
        level = struct.unpack("B", data[extra_start:extra_start + 1])[0]
        size_in_sstables = struct.unpack("H", data[extra_start + 1: extra_start + 3])[0]
        size_out_sstables = struct.unpack("H", data[extra_start + 3: extra_start + 5])[0]

        input_sstables_start = extra_start + 5
        output_sstables_start = input_sstables_start + size_in_sstables
        output_sstables_end = output_sstables_start + size_out_sstables
        encoded_input_sstables = data[input_sstables_start:output_sstables_start]
        encoded_output_sstables = data[output_sstables_start:output_sstables_end]

        decoded_input_sstables = ManifestSSTablesBlock.from_bytes(data=encoded_input_sstables).sstables
        decoded_output_sstables = ManifestSSTablesBlock.from_bytes(data=encoded_output_sstables).sstables

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
