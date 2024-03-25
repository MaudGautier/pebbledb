import struct
from typing import Optional

from src.record import Record

INT_H_SIZE = 2


class Block:
    """This class handles encoding and decoding of Data Blocks.

    Each Data Block has the following format:
    +--------------------+-----------------------------+-------+
    |       Records      |             Meta            | Extra |
    +--------------------+-----------------------------+-------+
    | R1 | R2 | ... | Rn | offset_R1 | ... | offset_Rn | nb_R  |
    +--------------------+-----------------------------+-------+
    (R = Record)
    """

    def __init__(self, data: bytes, offsets: list[int]):
        self.data = data
        self.offsets = offsets

    @property
    def number_records(self) -> int:
        return len(self.offsets)

    @property
    def size(self):
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        offset_bytes = struct.pack("H" * len(self.offsets), *self.offsets)
        number_records = struct.pack("H", self.number_records)

        return self.data + offset_bytes + number_records

    @classmethod
    def from_bytes(cls, data: bytes) -> "Block":
        # Decode number of records
        nb_records_offset = len(data) - INT_H_SIZE
        nb_records = struct.unpack("H", data[nb_records_offset:])[0]

        if nb_records * INT_H_SIZE + INT_H_SIZE > len(data):
            raise ValueError("Data length does not match number of records indicated.")

        # Decode offsets
        offsets_start = nb_records_offset - (nb_records * INT_H_SIZE)
        offsets_format = "H" * nb_records
        offsets = list(struct.unpack(offsets_format, data[offsets_start:nb_records_offset]))

        # Decode records
        encoded_records = data[0:offsets_start]

        return cls(data=encoded_records, offsets=offsets)


class BlockBuilder:
    def __init__(self, target_size: Optional[int] = 65_536):
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

