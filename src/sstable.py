import struct
from typing import Optional, Iterator

from src.blocks import DataBlockBuilder, DataBlock, MetaBlock
from src.record import Record

INT_i_SIZE = 4


class SSTableFile:
    def __init__(self, path: str, data: bytes):
        self.path = path
        self._write(data=data)

    def _write(self, data: bytes):
        with open(self.path, "wb") as f:
            f.write(data)

    def read(self, start: int, end: int) -> bytes:
        with open(self.path, "rb") as f:
            f.seek(start)
            return f.read(end - start)


class SSTableEncoding:
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
        self.meta_blocks = meta_blocks
        self.data = data

    def write(self, path):
        with open(path, "wb") as f:
            encoded_sstable = self.to_bytes()
            f.write(encoded_sstable)

    def to_bytes(self) -> bytes:
        encoded_meta_blocks = b''.join([meta_block.to_bytes() for meta_block in self.meta_blocks])
        encoded_meta_block_offset = struct.pack("i", len(self.data))

        return self.data + encoded_meta_blocks + encoded_meta_block_offset

    @classmethod
    def from_bytes(cls, data) -> "SSTableEncoding":
        # Decode number of data blocks
        meta_block_offset_start = len(data) - INT_i_SIZE
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


class SSTable:
    def __init__(self, meta_blocks: list[MetaBlock], meta_block_offset: int, file: SSTableFile):
        self.file = file
        self.meta_blocks = meta_blocks
        self.meta_block_offset = meta_block_offset
        # self.bloom = BloomFilter(bits_size=8*1_000_000, nb_hash_functions=5)

    def __iter__(self) -> Iterator[Record]:
        for block_id in range(len(self.meta_blocks)):
            data_block = self.read_data_block(block_id=block_id)
            for encoded_record in data_block:
                record = Record.from_bytes(data=encoded_record)
                yield record

    def find_block_id(self, key: Record.Key) -> Optional[int]:
        for i, meta_block in enumerate(self.meta_blocks):
            if meta_block.last_key < key:
                continue
            if meta_block.first_key <= key <= meta_block.last_key:
                return i
            if key <= meta_block.first_key:
                return None

        return None
        # TODO later: Optimisation:
        # # Perform binary search in meta blocks to find in which block the key is likely to be
        # first_key = self.meta_blocks[0].first_key
        # last_key = self.meta_blocks[-1].last_key

    def read_data_block(self, block_id: int) -> DataBlock:
        start = self.meta_blocks[block_id].offset
        end = self.meta_blocks[block_id + 1].offset \
            if block_id + 1 < len(self.meta_blocks) \
            else self.meta_block_offset

        encoded_block = self.file.read(start=start, end=end)
        return DataBlock.from_bytes(data=encoded_block)

    # TODO: Probably return the record and move the decoding up in the LSM Storage part
    def get(self, key: Record.Key) -> Optional[Record.Value]:
        # logique:
        # 1. trouve le bon block (lire meta blocks => lequel) => offset du datablock
        # 2. dans block: trouve la clé: binary search sur les clés offsets
        block_id = self.find_block_id(key=key)
        if block_id is None:
            return None

        block = self.read_data_block(block_id=block_id)
        record = block.get(key=key)

        return record.value if record is not None else None


class SSTableBuilder:
    """This class handles the creation of SSTables.
    Its content is stored in an in-memory buffer that gets converted into an SSTable object only once it is full.
    """

    def __init__(self, sstable_size: Optional[int] = 262_144_000, block_size: Optional[int] = 65_536):
        # The usual target size of an SSTable is 256MB
        self.block_size = block_size
        self.data_buffer = bytearray(sstable_size)
        self.data_block_offsets = []
        self.block_builder = DataBlockBuilder(target_size=block_size)
        self.current_buffer_position = 0
        self.meta_blocks = []

    def add(self, key: Record.Key, value: Record.Value):
        """Adds a key-value pair to the SSTable.
        As long as the current block is not full, the record is appended to the current block.
        Once it is full, the block is created, the encoded block is added to the SSTable's buffer and a new block
        builder is initialized.
        """
        was_added = self.block_builder.add(key=key, value=value)

        # Nothing to do if the record was added to the block
        if was_added:
            return

        # Otherwise, finalize block
        self.finish_block()

        # Create a new block
        self.block_builder = DataBlockBuilder(target_size=self.block_size)

        # Add record to the new block
        self.block_builder.add(key=key, value=value)

    def finish_block(self) -> DataBlock:
        # Add current buffer position to list of block offsets
        self.data_block_offsets.append(self.current_buffer_position)

        # Add meta block
        meta_block = MetaBlock(first_key=self.block_builder.first_key,
                               last_key=self.block_builder.last_key,
                               offset=self.current_buffer_position)
        self.meta_blocks.append(meta_block)

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

    def build(self, path: str) -> SSTable:
        self.finish_block()

        # Write to file
        encoded_sstable = SSTableEncoding(data=bytes(self.data_buffer[:self.current_buffer_position]),
                                          meta_blocks=self.meta_blocks).to_bytes()

        file = SSTableFile(path=path, data=encoded_sstable)

        return SSTable(
            meta_blocks=self.meta_blocks,
            file=file,
            meta_block_offset=self.current_buffer_position)
