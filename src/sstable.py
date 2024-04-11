import os
import struct
from typing import Optional

from src.blocks import DataBlockBuilder, DataBlock, MetaBlock
from src.bloom_filter import BloomFilter
from src.iterators import SSTableIterator
from src.record import Record

INT_i_SIZE = 4


class SSTableFile:
    def __init__(self, path: str):
        self.path = path

    @classmethod
    def create(cls, path: str, data: bytes):
        obj = cls(path)
        if obj._exists():
            raise ValueError(f"Cannot create the file because there is already one at {path}")
        obj._write(data=data)
        return obj

    @classmethod
    def open(cls, path: str):
        obj = cls(path)
        if not obj._exists():
            raise ValueError(f"Cannot open the file because there is none at {path}")
        return obj

    def __eq__(self, other):
        if not isinstance(other, SSTableFile):
            return NotImplemented
        return self.path == other.path

    def _write(self, data: bytes):
        with open(self.path, "wb") as f:
            f.write(data)

    def read_range(self, start: int, end: int) -> bytes:
        with open(self.path, "rb") as f:
            f.seek(start)
            return f.read(end - start)

    def read(self) -> bytes:
        with open(self.path, "rb") as f:
            return f.read()

    def _exists(self) -> bool:
        return os.path.isfile(self.path)


class SSTableEncoding:
    """This class handles encoding and decoding of SSTables.

    Each SSTable has the following format:
    +-----------------------+---------------------------+--------------+----------------------------+
    |         Blocks        |        Meta Blocks        |  Meta Bloom  |            Extra           |
    +-----------------------+---------------------------+--------------+----------------------------+
    | DB1 | DB2 | ... | DBn | meta_DB1 | ... | meta_DBn | bloom filter | meta_offset | bloom_offset |
    +-----------------------+---------------------------+--------------+----------------------------+
    (DB = Data Block)
    """

    def __init__(self, data: bytes, meta_blocks: list[MetaBlock], bloom_filter: BloomFilter):
        self.meta_blocks = meta_blocks
        self.data = data
        self.bloom_filter = bloom_filter

    @property
    def meta_block_section_offset(self):
        return len(self.data)

    def write(self, path):
        with open(path, "wb") as f:
            encoded_sstable = self.to_bytes()
            f.write(encoded_sstable)

    def to_bytes(self) -> bytes:
        encoded_meta_blocks = b''.join([meta_block.to_bytes() for meta_block in self.meta_blocks])
        encoded_bloom_filter = self.bloom_filter.to_bytes()
        encoded_meta_block_offset = struct.pack("i", len(self.data))
        encoded_bloom_filter_offset = struct.pack("i", len(self.data) + len(encoded_meta_blocks))

        return self.data + encoded_meta_blocks + encoded_bloom_filter + encoded_meta_block_offset + encoded_bloom_filter_offset

    @classmethod
    def from_bytes(cls, data) -> "SSTableEncoding":
        # Decode extra
        extra_section_start = len(data) - 2 * INT_i_SIZE
        extra_section_end = len(data)
        extra_meta_offset = extra_section_start
        extra_bloom_offset = extra_section_start + INT_i_SIZE
        bloom_offset = struct.unpack("i", data[extra_bloom_offset:extra_section_end])[0]
        meta_block_offset = struct.unpack("i", data[extra_meta_offset:extra_bloom_offset])[0]

        # Decode bloom filters
        encoded_bloom_filter = data[bloom_offset:extra_section_start]
        bloom_filter = BloomFilter.from_bytes(data=encoded_bloom_filter)

        # Decode meta blocks
        encoded_meta_blocks = data[meta_block_offset:bloom_offset]
        meta_blocks = []
        while len(encoded_meta_blocks) > 0:
            meta_block = MetaBlock.from_bytes(data=encoded_meta_blocks)
            meta_blocks.append(meta_block)
            encoded_meta_blocks = encoded_meta_blocks[meta_block.size:]

        # Decode data blocks
        encoded_data_blocks = data[0:meta_block_offset]

        return cls(data=encoded_data_blocks, meta_blocks=meta_blocks, bloom_filter=bloom_filter)


class SSTable:
    # TODO: ne contenir que:
    #  first key, last key ??? (utile pour savoir si besoin de regarder dedans - plus tard quand compaction niveaux ) ,
    #  ✅ meta_blocks,
    #  ✅ meta_block_offset,
    #  ✅ file
    #  ----------------------------------------
    #  Iterator à part ???

    def __init__(self,
                 meta_blocks: list[MetaBlock],
                 meta_block_offset: int,
                 file: SSTableFile,
                 bloom_filter: BloomFilter,
                 first_key: Record.Key,
                 last_key: Record.Key
                 ):
        self.file = file
        self.meta_blocks = meta_blocks
        self.meta_block_offset = meta_block_offset
        self.bloom_filter = bloom_filter
        self.first_key = first_key
        self.last_key = last_key

    def __eq__(self, other):
        if not isinstance(other, SSTable):
            return NotImplemented
        return (self.file == other.file
                and self.meta_blocks == other.meta_blocks
                and self.meta_block_offset == other.meta_block_offset
                and self.bloom_filter == other.bloom_filter
                and self.first_key == other.first_key
                and self.last_key == other.last_key)

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

        encoded_block = self.file.read_range(start=start, end=end)
        return DataBlock.from_bytes(data=encoded_block)

    # TODO: Probably return the record and move the decoding up in the LSM Storage part
    def get(self, key: Record.Key) -> Optional[Record.Value]:
        """To look up a key in a SSTable, we need to:
        1. Find the block that may contain it (by parsing meta blocks first and last keys)
        2. Read the block and search for the key within the block.
        """
        block_id = self.find_block_id(key=key)
        if block_id is None:
            return None

        block = self.read_data_block(block_id=block_id)
        record = block.get(key=key)

        return record.value if record is not None else None

    def scan(self, lower: Record.Key, upper: Record.Key) -> SSTableIterator:
        return SSTableIterator(sstable=self, start_key=lower, end_key=upper)

    @classmethod
    def build_from_path(cls, path: str):
        file = SSTableFile.open(path=path)
        data = file.read()
        sstable_encoding = SSTableEncoding.from_bytes(data=data)

        first_key = sstable_encoding.meta_blocks[0].first_key
        last_key = sstable_encoding.meta_blocks[-1].last_key
        meta_blocks = sstable_encoding.meta_blocks
        meta_block_offset = sstable_encoding.meta_block_section_offset
        bloom_filter = sstable_encoding.bloom_filter

        return cls(meta_blocks=meta_blocks, meta_block_offset=meta_block_offset,
                   first_key=first_key, last_key=last_key,
                   bloom_filter=bloom_filter, file=file)


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
        self.keys = []

    def add(self, key: Record.Key, value: Record.Value):
        """Adds a key-value pair to the SSTable.
        As long as the current block is not full, the record is appended to the current block.
        Once it is full, the block is created, the encoded block is added to the SSTable's buffer and a new block
        builder is initialized.
        """
        self.keys.append(key)
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
        bloom_filter = BloomFilter.build_from_keys_and_fp_rate(keys=self.keys, fp_rate=0.001)
        encoded_sstable = SSTableEncoding(data=bytes(self.data_buffer[:self.current_buffer_position]),
                                          meta_blocks=self.meta_blocks,
                                          bloom_filter=bloom_filter).to_bytes()
        file = SSTableFile.create(path=path, data=encoded_sstable)

        # Return python object
        return SSTable(
            meta_blocks=self.meta_blocks,
            file=file,
            meta_block_offset=self.current_buffer_position,
            bloom_filter=bloom_filter,
            first_key=self.keys[0],
            last_key=self.keys[-1]
        )
