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


def test_iterate_on_memtable_with_boundaries():
    # GIVEN
    memtable = MemTable()
    for key in ["1", "4", "6", "9"]:
        memtable.put(key=key, value=key.encode(encoding="utf-8"))
    memtable_iterator = MemTableIterator(memtable=memtable, start_key="0", end_key="5")

    # WHEN
    scanned_records = list(item for item in memtable_iterator)

    # THEN
    expected_records = [
        Record(key="1", value="1".encode(encoding="utf-8")),
        Record(key="4", value="4".encode(encoding="utf-8"))
    ]
    assert scanned_records == expected_records


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


def test_data_block_select_index():
    # GIVEN
    encoded_record1 = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1'
    encoded_record2 = b'\x04\x00\x00\x00key2\x06\x00\x00\x00value2'
    encoded_record3 = b'\x04\x00\x00\x00key3\x06\x00\x00\x00value3'
    encoded_record4 = b'\x04\x00\x00\x00key4\x06\x00\x00\x00value4'

    block = DataBlock(data=encoded_record1 + encoded_record2 + encoded_record3 + encoded_record4,
                      offsets=[0, 18, 36, 54])
    data_block_iterator = DataBlockIterator(block=block)

    # WHEN
    # Inside boundaries
    index_key1 = data_block_iterator._select_index(key="key1")
    index_key2 = data_block_iterator._select_index(key="key2")
    index_key3 = data_block_iterator._select_index(key="key3")
    index_key4 = data_block_iterator._select_index(key="key4")
    # Outside boundaries
    index_key0 = data_block_iterator._select_index(key="key0")
    index_key8 = data_block_iterator._select_index(key="key8")

    # THEN
    # Inside boundaries
    assert index_key1 == 0
    assert index_key2 == 1
    assert index_key3 == 2
    assert index_key4 == 3

    # Outside boundaries
    assert index_key0 == 0
    assert index_key8 == 4


def test_iterate_on_data_block_with_boundaries_before():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=100)
    block_builder.add(key="key1", value=b'value1')
    block_builder.add(key="key2", value=b'value2')
    block_builder.add(key="key3", value=b'value3')
    block_builder.add(key="key4", value=b'value4')
    block = block_builder.create_block()
    data_block_iterator = DataBlockIterator(block=block, start_key="k", end_key="key0")

    # WHEN
    iterated_items = list(item for item in data_block_iterator)

    # THEN
    expected_items = []
    assert iterated_items == expected_items


def test_iterate_on_data_block_with_boundaries_after_returns_empty_list():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=100)
    block_builder.add(key="key1", value=b'value1')
    block_builder.add(key="key2", value=b'value2')
    block_builder.add(key="key3", value=b'value3')
    block_builder.add(key="key4", value=b'value4')
    block = block_builder.create_block()
    data_block_iterator = DataBlockIterator(block=block, start_key="key6", end_key="key9")

    # WHEN
    iterated_items = list(item for item in data_block_iterator)

    # THEN
    expected_items = []

    assert iterated_items == expected_items


def test_iterate_on_data_block_with_boundaries_inside_returns_partial_list():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=100)
    block_builder.add(key="key1", value=b'value1')
    block_builder.add(key="key2", value=b'value2')
    block_builder.add(key="key3", value=b'value3')
    block_builder.add(key="key4", value=b'value4')
    block = block_builder.create_block()
    data_block_iterator = DataBlockIterator(block=block, start_key="key2", end_key="key4")

    # WHEN
    iterated_items = list(item for item in data_block_iterator)

    # THEN
    expected_items = [
        Record(key="key2", value=b'value2'),
        Record(key="key3", value=b'value3'),
        Record(key="key4", value=b'value4'),
    ]
    assert iterated_items == expected_items


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
