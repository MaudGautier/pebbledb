import struct
import zlib
from typing import Optional


class Checksum:
    nb_bytes = 4

    def __init__(self, data: Optional[bytes] = None, checksum: Optional[int] = None):
        if checksum and data:
            raise ValueError(f"Both checksum ({checksum}) and data ({data}) were provided, there should be only one!")
        elif checksum:
            self.crc32 = checksum
        elif data:
            self.crc32 = zlib.crc32(data)
        else:
            raise ValueError(f"Neither checksum nor data was provided, one should be provided!")

    def __eq__(self, other: "Checksum") -> bool:
        if not isinstance(other, Checksum):
            return NotImplemented
        return self.crc32 == other.crc32

    def to_bytes(self) -> bytes:
        return struct.pack("I", self.crc32)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Checksum":
        checksum = struct.unpack("I", data)[0]
        return cls(checksum=checksum)
