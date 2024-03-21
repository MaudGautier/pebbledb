import struct
from typing import Optional

from src.record import Record


class Block:
    def __init__(self, data: bytes, offsets: list[int]):
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
        offsets = list(struct.unpack("H" * nb_records, data[- nb_records * 2 - 2:-2]))
        encoded_records = data[0:- nb_records * 2 - 2]

        return cls(data=encoded_records, offsets=offsets)


class BlockBuilder:
    def __init__(self, target_size: int = 65536):
        # target_size is the size of a page. In my arm64 M2 mac, it is 65536 bytes (obtained with `stat -f %k`)
        self.target_size = target_size
        self.offsets = []
        self.data_buffer = bytearray(self.target_size)
        self.data_length = 0

    def add(self, key: Record.Key, value: Record.Value) -> bool:
        encoded_record = Record(key=key, value=value).to_bytes()
        size = len(encoded_record)

        current_offset = self.data_length
        new_offset = current_offset + size

        if new_offset > self.target_size:
            return False

        self.offsets.append(current_offset)
        self.data_buffer[current_offset:new_offset] = encoded_record
        self.data_length += size

        return True

    def create_block(self) -> Block:
        return Block(data=bytes(self.data_buffer), offsets=self.offsets)

