import struct
from typing import Optional

from src.blocks import BlockBuilder, Block, MetaBlock
from src.record import Record

INT_i_SIZE = 4


class SSTable:
    """This class handles encoding and decoding of SSTables.

    Each SSTable has the following format:
    +-----------------------+---------------------------+-------+
    |         Blocks        |            Meta           | Extra |
    +-----------------------+---------------------------+-------+
    | DB1 | DB2 | ... | DBn | meta_DB1 | ... | meta_DBn | nb_DB |
    +-----------------------+---------------------------+-------+
    (DB = Data Block)
    """

    def __init__(self, data: bytes, meta_blocks: list[MetaBlock]):
        self.data = data
        self.meta_blocks = meta_blocks

    def write(self, path):
        with open(path, "wb") as f:
            encoded_sstable = self.to_bytes()
            f.write(encoded_sstable)

    def to_bytes(self) -> bytes:
        encoded_meta_blocks = b''.join([meta_block.to_bytes() for meta_block in self.meta_blocks])
        encoded_meta_block_offset = struct.pack("i", len(self.data))

        return self.data + encoded_meta_blocks + encoded_meta_block_offset

    @classmethod
    def from_bytes(cls, data) -> "SSTable":
        # Decode number of data blocks
        meta_block_offset_start = len(
            data) - INT_i_SIZE  # TODO: rename: c'est la position de l'endroit où on dit où est l'offset
        meta_block_offset = struct.unpack("i", data[meta_block_offset_start:])[0]

        # Decode meta blocks
        encoded_meta_blocks = data[meta_block_offset: meta_block_offset_start]
        meta_blocks = []
        while len(encoded_meta_blocks) > 0:
            meta_block = MetaBlock.from_bytes(data=encoded_meta_blocks)
            meta_blocks.append(meta_block)
            encoded_meta_blocks = encoded_meta_blocks[meta_block.size:]

        # Decode data blocks
        encoded_data_blocks = data[0:meta_block_offset]

        return cls(data=encoded_data_blocks, meta_blocks=meta_blocks)


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
