import pytest

from src.blocks import DataBlock, DataBlockBuilder
from src.iterators import DataBlockIterator, MemTableIterator, SSTableIterator
from src.memtable import MemTable
from src.record import Record
from src.__fixtures__.sstable import sstable_four_blocks, records_for_sstable_four_blocks


def test_iterate_on_memtable():
    # GIVEN
    memtable = MemTable()
    keys = ["1", "6", "9", "4"]
    for key in keys:
        memtable.put(key=key, value=key.encode(encoding="utf-8"))
    memtable_iterator = MemTableIterator(memtable=memtable)

    # WHEN
    records = list(item for item in memtable_iterator)

    # THEN
    expected_records = [Record(key=key, value=key.encode(encoding="utf-8")) for key in sorted(keys)]
    assert records == expected_records


def test_iterate_on_empty_memtable_raises_stop_iteration():
    # GIVEN
    memtable = MemTable()
    memtable_iterator = MemTableIterator(memtable=memtable)

    # WHEN/THEN
    with pytest.raises(StopIteration):
        next(memtable_iterator)


def test_iterate_on_data_block():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=100)
    block_builder.add(key="key1", value=b'value1')
    block_builder.add(key="key2", value=b'value2')
    block_builder.add(key="key3", value=b'value3')
    block_builder.add(key="key4", value=b'value4')
    block = block_builder.create_block()
    data_block_iterator = DataBlockIterator(block=block)

    # WHEN
    iterated_items = list(item for item in data_block_iterator)

    # THEN
    expected_items = [
        Record(key="key1", value=b'value1'),
        Record(key="key2", value=b'value2'),
        Record(key="key3", value=b'value3'),
        Record(key="key4", value=b'value4'),
    ]
    assert iterated_items == expected_items


def test_iterate_on_empty_data_block_raises_stop_iteration():
    # GIVEN
    block = DataBlock(data=b'', offsets=[])
    data_block_iterator = DataBlockIterator(block=block)

    # WHEN/THEN
    with pytest.raises(StopIteration):
        next(data_block_iterator)


def test_iterate_on_sstable(sstable_four_blocks, records_for_sstable_four_blocks):
    # GIVEN
    sstable_iterator = SSTableIterator(sstable=sstable_four_blocks)

    # WHEN
    iterated_records = list(item for item in sstable_iterator)

    # THEN
    assert iterated_records == records_for_sstable_four_blocks


def test_iterate_on_finished_sstable_raises_stop_iteration(sstable_four_blocks):
    # GIVEN
    sstable_iterator = SSTableIterator(sstable=sstable_four_blocks)

    # WHEN/THEN
    # Empty iterator
    list(item for item in sstable_iterator)
    # Next iteration should raise an error
    with pytest.raises(StopIteration):
        next(sstable_iterator)
