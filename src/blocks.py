import struct
from typing import Optional

from src.record import Record


class Block:
    def __init__(self, data: bytes, offsets: Optional[list[int]]):
        self.data = data
        self.offsets = offsets

    @property
    def number_records(self) -> int:
        return len(self.offsets)

    def to_bytes(self) -> bytes:
        offset_bytes = struct.pack("H" * len(self.offsets), *self.offsets)
        number_records = struct.pack("H", self.number_records)

        return self.data + offset_bytes + number_records

    @classmethod
    def from_bytes(cls, data: bytes) -> "Block":
        nb_records = struct.unpack("H", data[-2:])[0]
        offsets = struct.unpack("H" * nb_records, data[- nb_records * 2 - 2:-2])[0]
        encoded_records = data[0:- nb_records * 2 - 2]

        return cls(data=encoded_records, offsets=offsets)


class BlockBuilder:
    # target_size is the size of a page. In my arm64 M2 mac, it is 65536 bytes (obtained with `stat -f %k`)
    block_target_size = 65536

    def __init__(self):
        self.offsets = []
        self.data_buffer = bytearray(self.block_target_size)  # []  # BUFFER bytearray(self.target_size)

    @property
    def current_offset(self) -> int:
        return self.offsets[-1] if len(self.offsets) > 0 else 0

    def add(self, key: Record.Key, value: Record.Value) -> bool:
        encoded_record = Record(key=key, value=value).to_bytes()
        size = len(encoded_record)

        new_offset = self.current_offset + size

        if new_offset > self.block_target_size:
            return False

        self.offsets.append(new_offset)
        self.data_buffer[self.current_offset:new_offset] = encoded_record

        return True

    def create_block(self) -> Block:
        return Block(data=bytes(self.data_buffer), offsets=self.offsets)

