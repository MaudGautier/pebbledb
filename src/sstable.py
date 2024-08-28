import os
import struct
from typing import Optional

from src.blocks import DataBlockBuilder, DataBlock, MetaBlock
from src.bloom_filter import BloomFilter
from src.iterators import ScanSSTableIterator
from src.record import Record

INT_i_SIZE = 4


class SSTableFile:
    Path = str

    def __init__(self, path: Path):
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

    def remove_self(self):
        os.remove(self.path)


class SSTableEncoding:
    """This class handles encoding and decoding of SSTables.

    Each SSTable has the following format:
    +-----------------------+---------------------------+--------------+------------------------------------------+
    |         Blocks        |        Meta Blocks        |  Meta Bloom  |                   Extra                  |
    +-----------------------+---------------------------+--------------+------------------------------------------+
    | DB1 | DB2 | ... | DBn | meta_DB1 | ... | meta_DBn | bloom filter | meta_offset | bloom_offset | max_seq_num |
    +-----------------------+---------------------------+--------------+------------------------------------------+
    (DB = Data Block)
    """

    def __init__(self, data: bytes, meta_blocks: list[MetaBlock], bloom_filter: BloomFilter, max_sequence_number: int):
        self.meta_blocks = meta_blocks
        self.data = data
        self.bloom_filter = bloom_filter
        self.max_sequence_number = max_sequence_number

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
        encoded_max_seq_num = struct.pack("I", self.max_sequence_number)
        encoded_extra = encoded_meta_block_offset + encoded_bloom_filter_offset + encoded_max_seq_num

        return self.data + encoded_meta_blocks + encoded_bloom_filter + encoded_extra

    @classmethod
    def from_bytes(cls, data) -> "SSTableEncoding":
        # Decode extra
        extra_section_start = len(data) - 3 * INT_i_SIZE
        extra_section_end = len(data)
        extra_meta_offset = extra_section_start
        extra_bloom_offset = extra_section_start + INT_i_SIZE
        extra_max_sequence_number_offset = extra_section_start + 2 * INT_i_SIZE
        max_sequence_number = struct.unpack("i", data[extra_max_sequence_number_offset:extra_section_end])[0]
        bloom_offset = struct.unpack("i", data[extra_bloom_offset:extra_max_sequence_number_offset])[0]
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

        return cls(data=encoded_data_blocks, meta_blocks=meta_blocks, bloom_filter=bloom_filter,
                   max_sequence_number=max_sequence_number)


class SSTable:
    def __init__(self,
                 meta_blocks: list[MetaBlock],
                 meta_block_offset: int,
                 file: SSTableFile,
                 bloom_filter: BloomFilter,
                 first_key: Record.Key,
                 last_key: Record.Key,
                 max_sequence_number: int,
                 ):
        self.file = file
        self.meta_blocks = meta_blocks
        self.meta_block_offset = meta_block_offset
        self.bloom_filter = bloom_filter
        self.first_key = first_key
        self.last_key = last_key
        self.max_sequence_number = max_sequence_number

    def __eq__(self, other):
        if not isinstance(other, SSTable):
            return NotImplemented
        return (self.file == other.file
                and self.meta_blocks == other.meta_blocks
                and self.meta_block_offset == other.meta_block_offset
                and self.bloom_filter == other.bloom_filter
                and self.first_key == other.first_key
                and self.last_key == other.last_key)

    def find_blocks_ids(self, key: Record.Key) -> list[int]:
        blocks_ids = []
        for i, meta_block in enumerate(self.meta_blocks):
            if meta_block.last_key < key:
                continue
            if meta_block.first_key <= key <= meta_block.last_key:
                blocks_ids.append(i)
            if key <= meta_block.first_key:
                return blocks_ids

        return blocks_ids

    def read_data_block(self, block_id: int) -> DataBlock:
        start = self.meta_blocks[block_id].offset
        end = self.meta_blocks[block_id + 1].offset \
            if block_id + 1 < len(self.meta_blocks) \
            else self.meta_block_offset

        encoded_block = self.file.read_range(start=start, end=end)
        return DataBlock.from_bytes(data=encoded_block)

    # TODO: Probably return the record and move the decoding up in the LSM Storage part
    def get(self, key: Record.Key, snapshot: Optional[int] = None) -> Optional[Record.Value]:
        """To look up a key in a SSTable, we need to:
        1. Find the blocks that may contain it (by parsing meta blocks first and last keys)
        2. Read the blocks and search for the key within each block.
        3. Select the version that is the most recent, but before the snapshot.
        """
        blocks_ids = self.find_blocks_ids(key=key)
        if not blocks_ids:
            return None

        for block_id in blocks_ids:
            block = self.read_data_block(block_id=block_id)
            record = block.get(key=key, snapshot=snapshot)
            if record is not None:
                return record.value

        return None

    def scan(self, lower: Record.Key, upper: Record.Key, snapshot: Optional[int] = None) -> ScanSSTableIterator:
        return ScanSSTableIterator(sstable=self, start_key=lower, end_key=upper, snapshot=snapshot)

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
        max_sequence_number = sstable_encoding.max_sequence_number

        return cls(meta_blocks=meta_blocks, meta_block_offset=meta_block_offset,
                   first_key=first_key, last_key=last_key,
                   bloom_filter=bloom_filter, file=file, max_sequence_number=max_sequence_number)


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
        self.max_sequence_number = 0

    def add(self, record: Record):
        """Adds a key-value pair to the SSTable.
        As long as the current block is not full, the record is appended to the current block.
        Once it is full, the block is created, the encoded block is added to the SSTable's buffer and a new block
        builder is initialized.
        """
        self.keys.append(record.key)
        self.max_sequence_number = max(self.max_sequence_number, record.sequence_number)
        was_added = self.block_builder.add(record=record)

        # Nothing to do if the record was added to the block
        if was_added:
            return

        # Otherwise, finalize block
        self.finish_block()

        # Create a new block
        self.block_builder = DataBlockBuilder(target_size=self.block_size)

        # Add record to the new block
        self.block_builder.add(record=record)

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
                                          bloom_filter=bloom_filter,
                                          max_sequence_number=self.max_sequence_number).to_bytes()
        file = SSTableFile.create(path=path, data=encoded_sstable)

        # Return python object
        return SSTable(
            meta_blocks=self.meta_blocks,
            file=file,
            meta_block_offset=self.current_buffer_position,
            bloom_filter=bloom_filter,
            first_key=self.keys[0],
            last_key=self.keys[-1],
            max_sequence_number=self.max_sequence_number
        )
