import struct
from typing import Optional

from src.blocks import BlockBuilder, Block
from src.record import Record

INT_i_SIZE = 4


class SSTable:
    """This class handles encoding and decoding of SSTables.

    Each SSTable has the following format:
    +-----------------------+-------------------------------+-------+
    |         Blocks        |              Meta             | Extra |
    +-----------------------+-------------------------------+-------+
    | DB1 | DB2 | ... | DBn | offset_DB1 | ... | offset_DBn | nb_DB |
    +-----------------------+-------------------------------+-------+
    (DB = Data Block)
    """

    def __init__(self, data: bytes, offsets: list[int]):
        self.data = data
        self.offsets = offsets

    @property
    def number_data_blocks(self) -> int:
        return len(self.offsets)

    def write(self, path):
        with open(path, "wb") as f:
            encoded_sstable = self.to_bytes()
            f.write(encoded_sstable)

    def to_bytes(self) -> bytes:
        encoded_offsets = struct.pack("i" * self.number_data_blocks, *self.offsets)
        encoded_nb_data_blocks = struct.pack("i", self.number_data_blocks)

        return self.data + encoded_offsets + encoded_nb_data_blocks

    @classmethod
    def from_bytes(cls, data) -> "SSTable":
        # Decode number of data blocks
        nb_data_blocks_offset = len(data) - INT_i_SIZE
        nb_data_blocks = struct.unpack("i", data[nb_data_blocks_offset:])[0]

        if nb_data_blocks * INT_i_SIZE + INT_i_SIZE > len(data):
            raise ValueError("Data length does not match number of data blocks indicated.")

        # Decode offsets
        offsets_start = nb_data_blocks_offset - (nb_data_blocks * INT_i_SIZE)
        offsets_format = "i" * nb_data_blocks
        offsets = list(struct.unpack(offsets_format, data[offsets_start:nb_data_blocks_offset]))

        # Decode data blocks
        encoded_data_blocks = data[0:offsets_start]

        return cls(data=encoded_data_blocks, offsets=offsets)


class SSTableBuilder:
    """This class handles the creation of SSTables.
    Its content is stored in an in-memory buffer that gets converted into an SSTable object only once it is full.
    """

    def __init__(self, sstable_size: Optional[int] = 262_144_000, block_size: Optional[int] = 65_536):
        # The usual target size of an SSTable is 256MB
        self.sstable_size = sstable_size
        self.data_buffer = bytearray(self.sstable_size)
        self.data_block_offsets = []
        self.block_builder = BlockBuilder(target_size=block_size)
        self.current_buffer_position = 0

    def add(self, key: Record.Key, value: Record.Value):
        """Adds a key-value pair to the SSTable.
        As long as the current block is not full, the record is appended to the current block.
        Once it is full, the block is created, the encoded block is added to the SSTable's buffer and a new block
        builder is initialized.
        """
        was_added = self.block_builder.add(key=key, value=value)

        # Nothing to do if the record was added to the block
        if was_added:
            # TODO: later: will need to get the info of the last key and keep it (useful for search in the SStable)
            return

        # Otherwise, finalize block
        self.finish_block()

        # Create a new block
        self.block_builder = BlockBuilder(target_size=self.sstable_size)

        # Add record to the new block
        self.block_builder.add(key=key, value=value)

    def finish_block(self) -> Block:
        # Add current buffer position to list of block offsets
        self.data_block_offsets.append(self.current_buffer_position)

        # Create block
        block = self.block_builder.create_block()
        encoded_block = block.to_bytes()

        # Add new encoded block to buffer
        start = self.current_buffer_position
        end = self.current_buffer_position + block.size
        self.data_buffer[start:end] = encoded_block

        # Update buffer position
        self.current_buffer_position += block.size

        return block

    def build(self):
        self.finish_block()
        return SSTable(data=bytes(self.data_buffer[:self.current_buffer_position]), offsets=self.data_block_offsets)
