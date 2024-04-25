import random
from contextlib import nullcontext as does_not_raise

import pytest

from src.blocks import DataBlock, MetaBlock
from src.bloom_filter import BloomFilter
from src.record import Record
from src.sstable import SSTableBuilder, SSTableEncoding, SSTable, SSTableFile


def test_add_record_to_current_block():
    # GIVEN
    records = [
        Record(key=b'key1', value=b'value1'),
        Record(key=b'key2', value=b'value2'),
    ]
    record_size = Record(key=b'keyN', value=b"valueN").size
    random_block_size = random.randint(len(records) * record_size, (len(records) + 1) * record_size - 1)
    sstable_size = random_block_size * 3
    sstable_builder = SSTableBuilder(sstable_size=sstable_size, block_size=random_block_size)

    # WHEN
    for record in records:
        sstable_builder.add(record=record)

    # THEN
    assert sstable_builder.current_buffer_position == 0
    assert sstable_builder.data_block_offsets == []
    assert sstable_builder.data_buffer == bytearray(sstable_size)


def test_adding_record_to_new_block_updates_buffer():
    # GIVEN
    records = [
        Record(key=b'key1', value=b'value1'),
        Record(key=b'key2', value=b'value2'),
        Record(key=b'key3', value=b'value3'),
    ]
    record_size = Record(key=b'keyN', value=b"valueN").size
    random_block_size = random.randint((len(records) - 1) * record_size, len(records) * record_size - 1)
    sstable_builder = SSTableBuilder(sstable_size=random_block_size * 3, block_size=random_block_size)

    # WHEN
    for record in records:
        assert sstable_builder.current_buffer_position == 0
        sstable_builder.add(record=record)

    # THEN
    record_index_size = 2  # Number of bytes for a "H" integer (Blocks)
    nb_records = 2
    nb_records_size = 2  # Number of bytes for a "H" integer (Blocks)
    block_index_size = nb_records * record_index_size + nb_records_size
    assert sstable_builder.current_buffer_position == 2 * record_size + block_index_size
    assert sstable_builder.data_block_offsets == [0]


def test_encode_sstable():
    # GIVEN
    data1 = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x04\x00\x00\x00key2\x06\x00\x00\x00value2'
    block1 = DataBlock(data=data1, offsets=[0, 18])
    encoded_block1 = block1.to_bytes()
    data2 = b'\x04\x00\x00\x00key3\x06\x00\x00\x00value3'
    block2 = DataBlock(data=data2, offsets=[0])
    encoded_block2 = block2.to_bytes()
    data = encoded_block1 + encoded_block2
    meta_block1 = MetaBlock(first_key=b'key1', last_key=b'key2', offset=0)
    meta_block2 = MetaBlock(first_key=b'key3', last_key=b'key3', offset=42)  # 42 = 18*2 + 2*2 + 2
    bloom_filter = BloomFilter.build_from_keys_and_fp_rate([b"key1", b"key2", b"key3"], fp_rate=0.0001)
    sstable = SSTableEncoding(data=data, meta_blocks=[meta_block1, meta_block2], bloom_filter=bloom_filter)

    # WHEN
    encoded_sstable = sstable.to_bytes()

    # THEN
    encoded_meta_block1 = meta_block1.to_bytes()
    encoded_meta_block2 = meta_block2.to_bytes()
    encoded_meta_blocks = b''.join([encoded_meta_block1, encoded_meta_block2])
    encoded_64 = b'@\x00\x00\x00'  # 64 = len(data) = 42 + 18 + 2 + 2
    encoded_meta_block_offset = encoded_64
    encoded_bloom_filter = bloom_filter.to_bytes()
    encoded_96 = b'`\x00\x00\x00'  # 96 = len(data + encoded_meta_blocks)
    encoded_bloom_filter_offset = encoded_96
    assert encoded_sstable == data + encoded_meta_blocks + encoded_bloom_filter + encoded_meta_block_offset + encoded_bloom_filter_offset


def test_decode_sstable():
    # GIVEN
    encoded_data = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x04\x00\x00\x00key2\x06\x00\x00\x00value2\x00\x00\x12\x00\x02\x00\x04\x00\x00\x00key3\x06\x00\x00\x00value3\x00\x00\x01\x00'
    encoded_meta_block1 = b'\x04\x00key1\x04\x00key2\x00\x00\x00\x00'
    encoded_meta_block2 = b'\x04\x00key3\x04\x00key3*\x00\x00\x00'
    encoded_bloom_filter = b'9\x02'
    encoded_meta_block_offset = b'@\x00\x00\x00'
    encoded_bloom_filter_offset = b'`\x00\x00\x00'
    encoded_meta_blocks = encoded_meta_block1 + encoded_meta_block2
    data = encoded_data + encoded_meta_blocks + encoded_bloom_filter + encoded_meta_block_offset + encoded_bloom_filter_offset

    # WHEN
    decoded_sstable = SSTableEncoding.from_bytes(data)

    # THEN
    assert decoded_sstable.data == encoded_data
    actual_encoded_meta_blocks = b''.join([meta_block.to_bytes() for meta_block in decoded_sstable.meta_blocks])
    assert encoded_meta_blocks == actual_encoded_meta_blocks


def test_find_block_of_key(sstable_four_blocks):
    # GIVEN
    sstable = sstable_four_blocks

    # WHEN/THEN
    assert sstable.find_block_id(b'ddd') == 0
    assert sstable.find_block_id(b'eee') == 1
    assert sstable.find_block_id(b'jj') == 2
    assert sstable.find_block_id(b'ooo') == 3
    assert sstable.find_block_id(b'a') is None
    assert sstable.find_block_id(b'iiii') == 2
    assert sstable.find_block_id(b'zzz') is None


def test_read_data_block(sstable_four_blocks, records_for_sstable_four_blocks):
    # GIVEN
    sstable = sstable_four_blocks

    # WHEN
    data_block = sstable.read_data_block(block_id=1)

    # THEN
    record1 = b'\x03\x00\x00\x00eee\xff\xff\xff\xff\xff\xff\xff\xfb\x17\x00\x00\x00some_long_value_for_eee'
    record2 = b'\x03\x00\x00\x00fff\xff\xff\xff\xff\xff\xff\xff\xfa\x17\x00\x00\x00some_long_value_for_fff'
    record3 = b'\x03\x00\x00\x00ggg\xff\xff\xff\xff\xff\xff\xff\xf9\x17\x00\x00\x00some_long_value_for_ggg'
    record4 = b'\x03\x00\x00\x00hhh\xff\xff\xff\xff\xff\xff\xff\xf8\x17\x00\x00\x00some_long_value_for_hhh'
    record_size = records_for_sstable_four_blocks[0].size

    assert data_block.number_records == 4
    assert data_block.data == b''.join([record1, record2, record3, record4])
    assert data_block.offsets == [0, record_size, 2 * record_size, 3 * record_size]


def test_get_key(sstable_four_blocks, records_for_sstable_four_blocks):
    # GIVEN
    sstable = sstable_four_blocks

    # WHEN/THEN
    for record in records_for_sstable_four_blocks:
        assert sstable.get(record.key) == record.value
    # Missing records
    assert sstable.get(b'jj') is None
    assert sstable.get(b'a') is None
    assert sstable.get(b'iiii') is None
    assert sstable.get(b'zzz') is None


@pytest.mark.parametrize(
    ("start_key", "end_key"),
    [
        pytest.param(
            b'cc', b'eee',
            id="inside",
        ),
        pytest.param(
            b'a', b'ccc',
            id="overlap-below",
        ),
        pytest.param(
            b'a', b'aa',
            id="outside-below",
        ),
        pytest.param(
            b'dd', b'zzzz',
            id="overlap-above",
        ),
        pytest.param(
            b'ww', b'zzzz',
            id="outside-above",
        ),
    ],
)
def test_scan_sstable(start_key, end_key, sstable_four_blocks, records_for_sstable_four_blocks):
    # GIVEN
    sstable = sstable_four_blocks

    # WHEN
    start_key, end_key = b"cc", b"eee"
    scanned_records_inside = list(record for record in sstable.scan(lower=start_key, upper=end_key))

    # THEN
    assert scanned_records_inside == [record for record in records_for_sstable_four_blocks if
                                      start_key <= record.key <= end_key]


def test_create_sstable_file_object_from_existing_path_should_raise_an_error(sstable_four_blocks):
    # GIVEN
    path_with_file = sstable_four_blocks.file.path

    # WHEN/THEN
    with pytest.raises(ValueError):
        SSTableFile.create(path=path_with_file, data=b'')


def test_open_sstable_file_object_from_existing_path_should_not_raise_an_error(sstable_four_blocks):
    # GIVEN
    path_with_file = sstable_four_blocks.file.path

    # WHEN/THEN
    with does_not_raise():
        SSTableFile.open(path=path_with_file)


def test_create_sstable_file_object_from_new_path_should_not_raise_an_error(temporary_sstable_path):
    # GIVEN
    path_with_no_file = temporary_sstable_path

    # WHEN/THEN
    with does_not_raise():
        SSTableFile.create(path=path_with_no_file, data=b'')


def test_open_sstable_file_object_from_new_path_should_raise_an_error(temporary_sstable_path):
    # GIVEN
    path_with_no_file = temporary_sstable_path

    # WHEN/THEN
    with pytest.raises(ValueError):
        SSTableFile.open(path=path_with_no_file)


def test_sstable_files_are_equal(temporary_sstable_path):
    # GIVEN
    SSTableFile.create(path=temporary_sstable_path, data=b'')
    file1 = SSTableFile.open(path=temporary_sstable_path)
    file2 = SSTableFile.open(path=temporary_sstable_path)

    # WHEN/THEN
    assert file1 == file2


def test_sstable_files_are_not_equal(temporary_sstable_path, temporary_sstable_path_2):
    # GIVEN
    SSTableFile.create(path=temporary_sstable_path, data=b'')
    SSTableFile.create(path=temporary_sstable_path_2, data=b'')
    file1 = SSTableFile.open(path=temporary_sstable_path)
    file2 = SSTableFile.open(path=temporary_sstable_path_2)

    # WHEN/THEN
    assert file1 != file2


def test_sstables_are_equal(temporary_sstable_path, simple_bloom_filter):
    # GIVEN
    SSTableFile.create(path=temporary_sstable_path, data=b'')
    file1 = SSTableFile.open(path=temporary_sstable_path)
    file2 = SSTableFile.open(path=temporary_sstable_path)
    sstable_1 = SSTable(meta_blocks=[], first_key=b'key1', last_key=b'key3', meta_block_offset=10,
                        bloom_filter=simple_bloom_filter, file=file1)
    sstable_2 = SSTable(meta_blocks=[], first_key=b'key1', last_key=b'key3', meta_block_offset=10,
                        bloom_filter=simple_bloom_filter, file=file2)

    # WHEN
    are_equal = sstable_1 == sstable_2

    # THEN
    assert are_equal is True


def test_sstables_are_not_equal_under_several_conditions(temporary_sstable_path, simple_bloom_filter,
                                                         simple_bloom_filter_2, sstable_four_blocks):
    # GIVEN
    SSTableFile.create(path=temporary_sstable_path, data=b'')
    meta_block = MetaBlock(first_key=b'key1', last_key=b'key3', offset=0)
    meta_block_other = MetaBlock(first_key=b'key1', last_key=b'key3', offset=2)
    sstable = SSTable(meta_blocks=[meta_block], first_key=b'key1', last_key=b'key3', meta_block_offset=10,
                      bloom_filter=simple_bloom_filter, file=SSTableFile.open(path=temporary_sstable_path))
    sstable_first_key = SSTable(meta_blocks=[meta_block], first_key=b'key2', last_key=b'key3', meta_block_offset=10,
                                bloom_filter=simple_bloom_filter, file=SSTableFile.open(path=temporary_sstable_path))
    sstable_last_key = SSTable(meta_blocks=[meta_block], first_key=b'key1', last_key=b'key4', meta_block_offset=10,
                               bloom_filter=simple_bloom_filter, file=SSTableFile.open(path=temporary_sstable_path))
    sstable_meta_block = SSTable(meta_blocks=[meta_block_other], first_key=b'key1', last_key=b'key3',
                                 meta_block_offset=10,
                                 bloom_filter=simple_bloom_filter, file=SSTableFile.open(path=temporary_sstable_path))
    sstable_offset = SSTable(meta_blocks=[meta_block], first_key=b'key1', last_key=b'key3', meta_block_offset=11,
                             bloom_filter=simple_bloom_filter, file=SSTableFile.open(path=temporary_sstable_path))
    sstable_bloom = SSTable(meta_blocks=[meta_block], first_key=b'key1', last_key=b'key3', meta_block_offset=10,
                            bloom_filter=simple_bloom_filter_2, file=SSTableFile.open(path=temporary_sstable_path))
    sstable_file = SSTable(meta_blocks=[meta_block], first_key=b'key1', last_key=b'key3', meta_block_offset=10,
                           bloom_filter=simple_bloom_filter, file=SSTableFile.open(path=sstable_four_blocks.file.path))

    # WHEN
    are_equal_first_key = sstable == sstable_first_key
    are_equal_last_key = sstable == sstable_last_key
    are_equal_meta_block = sstable == sstable_meta_block
    are_equal_offset = sstable == sstable_offset
    are_equal_bloom = sstable == sstable_bloom
    are_equal_file = sstable == sstable_file

    # THEN
    assert are_equal_first_key is False
    assert are_equal_last_key is False
    assert are_equal_meta_block is False
    assert are_equal_offset is False
    assert are_equal_bloom is False
    assert are_equal_file is False


def test_read_sstable_file(sstable_file_1, content_of_sstable_file_1):
    # GIVEN
    sstable_file = sstable_file_1

    # WHEN
    content = sstable_file.read()

    # THEN
    assert content == content_of_sstable_file_1


def test_read_range_sstable_file(sstable_file_1, content_of_sstable_file_1):
    # GIVEN
    sstable_file = sstable_file_1

    # WHEN
    content = sstable_file.read_range(start=8, end=19)

    # THEN
    assert content == content_of_sstable_file_1[8:19]


def test_reconstruct_a_sstable_from_file(sstable_four_blocks):
    # GIVEN
    original_sstable = sstable_four_blocks
    path = original_sstable.file.path

    # WHEN
    reconstructed_sstable = SSTable.build_from_path(path=path)

    # THEN
    assert reconstructed_sstable == original_sstable
