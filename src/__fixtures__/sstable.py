import os

import pytest

from src.record import Record
from src.sstable import SSTableBuilder

TEST_FIXTURES = "./test_fixtures"

if not os.path.exists(TEST_FIXTURES):
    os.makedirs(TEST_FIXTURES)


@pytest.fixture
def records_for_sstable_four_blocks():
    return [
        # Goes into Data Block 0
        Record(key="aaa", value=b'some_long_value_for_aaa'),
        Record(key="bbb", value=b'some_long_value_for_bbb'),
        Record(key="ccc", value=b'some_long_value_for_ccc'),
        Record(key="ddd", value=b'some_long_value_for_ddd'),
        # Goes into Data Block 1
        Record(key="eee", value=b'some_long_value_for_eee'),
        Record(key="fff", value=b'some_long_value_for_fff'),
        Record(key="ggg", value=b'some_long_value_for_ggg'),
        Record(key="hhh", value=b'some_long_value_for_hhh'),
        # Goes into Data Block 2
        Record(key="iii", value=b'some_long_value_for_iii'),
        Record(key="jjj", value=b'some_long_value_for_jjj'),
        Record(key="kkk", value=b'some_long_value_for_kkk'),
        Record(key="lll", value=b'some_long_value_for_lll'),
        # Goes into Data Block 3
        Record(key="mmm", value=b'some_long_value_for_mmm'),
        Record(key="nnn", value=b'some_long_value_for_nnn'),
        Record(key="ooo", value=b'some_long_value_for_ooo'),
        Record(key="ppp", value=b'some_long_value_for_ppp')
    ]


@pytest.fixture
def sstable_four_blocks(records_for_sstable_four_blocks):
    sstable_builder = SSTableBuilder(sstable_size=20000, block_size=150)
    for record in records_for_sstable_four_blocks:
        sstable_builder.add(key=record.key, value=record.value)

    sstable = sstable_builder.build(path=f"{TEST_FIXTURES}/sstable_four_blocks.sst")

    assert len(sstable.meta_blocks) == 4
    assert sstable.meta_blocks[0].first_key == "aaa"
    assert sstable.meta_blocks[0].last_key == "ddd"
    assert sstable.meta_blocks[1].first_key == "eee"
    assert sstable.meta_blocks[1].last_key == "hhh"
    assert sstable.meta_blocks[2].first_key == "iii"
    assert sstable.meta_blocks[2].last_key == "lll"
    assert sstable.meta_blocks[3].first_key == "mmm"
    assert sstable.meta_blocks[3].last_key == "ppp"

    return sstable


@pytest.fixture
def records_for_sstable_one_block():
    return [
        # Goes into Data Block 0
        Record(key="key1", value=b'value1'),
        Record(key="key2", value=b'value2'),
        Record(key="key3", value=b'value3')
    ]


@pytest.fixture
def sstable_one_block(records_for_sstable_one_block):
    sstable_builder = SSTableBuilder(sstable_size=20000, block_size=150)
    for record in records_for_sstable_one_block:
        sstable_builder.add(key=record.key, value=record.value)

    sstable = sstable_builder.build(path=f"{TEST_FIXTURES}/sstable_one_block.sst")

    assert len(sstable.meta_blocks) == 1
    assert sstable.meta_blocks[0].first_key == "key1"
    assert sstable.meta_blocks[0].last_key == "key3"

    return sstable


@pytest.fixture
def records_for_sstable_one_block_2():
    return [
        # Goes into Data Block 0
        Record(key="key1", value=b'value1'),
        Record(key="key2", value=b'value2'),
        Record(key="key3", value=b'value3')
    ]


@pytest.fixture
def sstable_one_block_2(records_for_sstable_one_block_2):
    sstable_builder = SSTableBuilder(sstable_size=20000, block_size=150)
    for record in records_for_sstable_one_block_2:
        sstable_builder.add(key=record.key, value=record.value)

    sstable = sstable_builder.build(path=f"{TEST_FIXTURES}/sstable_one_block.sst")

    assert len(sstable.meta_blocks) == 1
    assert sstable.meta_blocks[0].first_key == "key1"
    assert sstable.meta_blocks[0].last_key == "key3"

    return sstable
