import struct
from typing import Optional, Tuple

from src.sequence_number_generator import SequenceNumberGenerator

MAX_SNAPSHOT = (2 ** 8) ** 8


class Record:
    """This class handles encoding and decoding of Records.

    Records are stored in both the MemTable (in memory) and in the SSTable (on disk).
    It is a pair of key-value. The key is a string, and the value is in bytes (encoded and decoded by a layer above).

    Each Record has the following format:
    +----------+----------------+-----------------+------------+------------------+
    | Key_size |       Key      | Sequence Number | Value_size |      Value       |
    +----------+----------------+-----------------+------------+------------------+
    | 4 bytes  | Key_size bytes |     8 bytes     |  4 bytes   | Value_size bytes |
    +----------+----------------+-----------------+------------+------------------+
    """
    Key = bytes
    Value = bytes
    SequenceNumber = int
    ENCODING = "utf-8"
    NB_BYTES_INTEGER = 4

    def __init__(self, key: Key, value: Value, sequence_number: Optional[SequenceNumber] = None):
        self.key = key
        self.value = value
        self.sequence_number = sequence_number if sequence_number is not None else next(SequenceNumberGenerator(0))
        self.key_size = len(self.key)
        self.value_size = len(self.value)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Record):
            return NotImplemented
        return self.key == other.key and self.value == other.value

    def __repr__(self):
        return f"{self.key}: {self.value} ({self.sequence_number})"

    def __lt__(self, other: "Record"):
        if not isinstance(other, Record):
            return NotImplemented
        return self.key < other.key

    def is_duplicate(self, other: "Record"):
        if not isinstance(other, Record):
            return TypeError(f"Expected Record, got {type(other).__name__}")
        return self.key == other.key

    @staticmethod
    def encode_integer(integer: int) -> bytes:
        return struct.pack("i", integer)

    @property
    def encoded_key_size(self) -> bytes:
        return self.encode_integer(self.key_size)

    @property
    def encoded_value_size(self) -> bytes:
        return self.encode_integer(self.value_size)

    @property
    def size(self) -> int:
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        encoded_key_size = self.encoded_key_size
        encoded_key = self.snapshot_key  # already encoded
        encoded_value_size = self.encoded_value_size
        encoded_value = self.value  # already encoded

        return encoded_key_size + encoded_key + encoded_value_size + encoded_value

    @classmethod
    def decode_single_record(cls, data: bytes) -> tuple["Record", int]:
        key_size_end = cls.NB_BYTES_INTEGER
        key_size = struct.unpack("i", data[:key_size_end])[0]
        snapshot_key_end = key_size_end + key_size + 8
        key, sequence_number = cls.decode_snapshot_key(data=data[key_size_end:snapshot_key_end])
        value_size_end = snapshot_key_end + cls.NB_BYTES_INTEGER
        value_size = struct.unpack("i", data[snapshot_key_end:value_size_end])[0]
        value_end = value_size_end + value_size
        value = data[value_size_end:value_end]

        return cls(key=key, value=value, sequence_number=sequence_number), value_end

    @classmethod
    def from_bytes(cls, data: bytes) -> "Record":
        record, _ = cls.decode_single_record(data=data)
        return record

    @property
    def snapshot_key(self) -> bytes:
        """A `snapshot_key` combines the record's key and sequence number.
        Snapshot keys must be encoded in a way that preserves the targeted ordering, which is:
        - a lower key should be sorted before a higher key
        - if the keys are identical, then a higher sequence number should be sorted before a lower sequence number
          (because a higher sequence number means a more recent version).

        Therefore, the snapshot key is encoded by a concatenation of the key (thus, the main ordering element when
        sorting in lexicographical order) with a special encoding of the sequence number.

        To ensure that higher sequence numbers are sorted before lower sequence numbers, they are encoded with
        big-endianness (to have the most significant byte first) and bytes are inverted (by applying a bitwise NOT to
        each byte and a 0xFF mask).
        Therefore, the ordering becomes:
        ("A", 500) < ("A", 1) < ("B", 500) < ("B", 1)
        """
        # Encode the sequence number in big-endian format ('>Q' for big-endian unsigned long long)
        seq_num_bytes = struct.pack('>Q', self.sequence_number)

        # Invert the bytes of the sequence number (Bitwise NOT each byte and mask with 0xFF)
        inverted_seq_num_bytes = bytes(~byte & 0xFF for byte in seq_num_bytes)

        return self.key + inverted_seq_num_bytes

    @staticmethod
    def decode_snapshot_key(data: bytes) -> Tuple[Key, SequenceNumber]:
        key = data[:-8]
        inverted_seq_num_bytes = data[-8:]

        # Revert the inversion of the sequence number bytes
        seq_num_bytes = bytes(~byte & 0xFF for byte in inverted_seq_num_bytes)

        # Decode the sequence number from big-endian format
        sequence_number, = struct.unpack('>Q', seq_num_bytes)

        return key, sequence_number
