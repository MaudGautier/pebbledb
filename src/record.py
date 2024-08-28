import struct
from typing import Optional

from src.sequence_number_generator import SequenceNumberGenerator


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
        return f"{self.key}: {self.value}"

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
        encoded_key = self.key  # already encoded
        encoded_value_size = self.encoded_value_size
        encoded_value = self.value  # already encoded
        encoded_sequence_number = struct.pack("Q", self.sequence_number)

        return encoded_key_size + encoded_key + encoded_sequence_number + encoded_value_size + encoded_value

    @classmethod
    def decode_single_record(cls, data: bytes) -> tuple["Record", int]:
        key_size_end = cls.NB_BYTES_INTEGER
        key_size = struct.unpack("i", data[:key_size_end])[0]
        key_end = key_size_end + key_size
        key = data[key_size_end:key_end]
        sequence_number_end = key_end + 8
        decoded_sequence_number = struct.unpack("Q", data[key_end:sequence_number_end])[0]
        value_size_end = sequence_number_end + cls.NB_BYTES_INTEGER
        value_size = struct.unpack("i", data[sequence_number_end:value_size_end])[0]
        value_end = value_size_end + value_size
        value = data[value_size_end:value_end]

        return cls(key=key, value=value, sequence_number=decoded_sequence_number), value_end

    @classmethod
    def from_bytes(cls, data: bytes) -> "Record":
        record, _ = cls.decode_single_record(data=data)
        return record
