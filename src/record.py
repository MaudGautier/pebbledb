import struct


class Record:
    """This class handles encoding and decoding of Records.

    Records are stored in both the MemTable (in memory) and in the SSTable (on disk).
    It is a pair of key-value. The key is a string, and the value is in bytes (encoded and decoded by a layer above).

    Each Record has the following format:
    +----------+----------------+------------+------------------+
    | Key_size |       Key      | Value_size |      Value       |
    +----------+----------------+------------+------------------+
    | 4 bytes  | Key_size bytes |  4 bytes   | Value_size bytes |
    +----------+----------------+------------+------------------+
    """
    Key = str
    Value = bytes
    ENCODING = "utf-8"
    NB_BYTES_INTEGER = 4

    def __init__(self, key: Key, value: Value):
        self.key = key
        self.value = value
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

    @property
    def encoded_key(self) -> bytes:
        return bytes(self.key, encoding=self.ENCODING)

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
        encoded_key = self.encoded_key
        encoded_value_size = self.encoded_value_size
        encoded_value = self.value  # already encoded

        return encoded_key_size + encoded_key + encoded_value_size + encoded_value

    @classmethod
    def from_bytes(cls, data: bytes) -> "Record":
        key_size_end = cls.NB_BYTES_INTEGER
        key_size = struct.unpack("i", data[:key_size_end])[0]
        key_end = key_size_end + key_size
        key = data[key_size_end:key_end].decode(encoding=cls.ENCODING)
        value_size_end = key_end + cls.NB_BYTES_INTEGER
        value_size = struct.unpack("i", data[key_end:value_size_end])[0]
        value_end = value_size_end + value_size
        value = data[value_size_end:value_end]

        return cls(key=key, value=value)
