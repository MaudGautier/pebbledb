import os
import random
from typing import Generator

import pytest

from src.__fixtures__.constants import TEST_SSTABLE_FIXTURES_DIRECTORY
from src.record import Record
from src.sequence_number_generator import SequenceNumberGenerator
from src.sstable import SSTableBuilder, SSTable, SSTableFile


@pytest.fixture
def records_for_sstable_four_blocks():
    SequenceNumberGenerator.reset()
    return [
        # Goes into Data Block 0
        Record(key=b'aaa', value=b'some_long_value_for_aaa'),
        Record(key=b'bbb', value=b'some_long_value_for_bbb'),
        Record(key=b'ccc', value=b'some_long_value_for_ccc'),
        Record(key=b'ddd', value=b'some_long_value_for_ddd'),
        # Goes into Data Block 1
        Record(key=b'eee', value=b'some_long_value_for_eee'),
        Record(key=b'fff', value=b'some_long_value_for_fff'),
        Record(key=b'ggg', value=b'some_long_value_for_ggg'),
        Record(key=b'hhh', value=b'some_long_value_for_hhh'),
        # Goes into Data Block 2
        Record(key=b'iii', value=b'some_long_value_for_iii'),
        Record(key=b'jjj', value=b'some_long_value_for_jjj'),
        Record(key=b'kkk', value=b'some_long_value_for_kkk'),
        Record(key=b'lll', value=b'some_long_value_for_lll'),
        # Goes into Data Block 3
        Record(key=b'mmm', value=b'some_long_value_for_mmm'),
        Record(key=b'nnn', value=b'some_long_value_for_nnn'),
        Record(key=b'ooo', value=b'some_long_value_for_ooo'),
        Record(key=b'ppp', value=b'some_long_value_for_ppp')
    ]


@pytest.fixture
def sstable_four_blocks(records_for_sstable_four_blocks) -> Generator[SSTable, None, None]:
    SequenceNumberGenerator.reset()
    record_size = records_for_sstable_four_blocks[0].size
    block_size = random.randint(4 * record_size, 5 * record_size - 1)
    sstable_builder = SSTableBuilder(sstable_size=20000, block_size=block_size)
    for record in records_for_sstable_four_blocks:
        sstable_builder.add(record=record)

    sstable = sstable_builder.build(path=f"{TEST_SSTABLE_FIXTURES_DIRECTORY}/sstable_four_blocks.sst")

    assert len(sstable.meta_blocks) == 4
    assert sstable.meta_blocks[0].first_key == b'aaa'
    assert sstable.meta_blocks[0].last_key == b'ddd'
    assert sstable.meta_blocks[1].first_key == b'eee'
    assert sstable.meta_blocks[1].last_key == b'hhh'
    assert sstable.meta_blocks[2].first_key == b'iii'
    assert sstable.meta_blocks[2].last_key == b'lll'
    assert sstable.meta_blocks[3].first_key == b'mmm'
    assert sstable.meta_blocks[3].last_key == b'ppp'

    yield sstable

    # Cleanup code (Delete the file created by the fixture)
    os.remove(sstable.file.path)


@pytest.fixture
def records_for_sstable_one_block():
    SequenceNumberGenerator.reset()
    return [
        # Goes into Data Block 0
        Record(key=b'key1', value=b'value1'),
        Record(key=b'key2', value=b'value2'),
        Record(key=b'key3', value=b'value3')
    ]


def build_sstable_one_block(records_for_sstable_one_block, file_name) -> Generator[SSTable, None, None]:
    SequenceNumberGenerator.reset()
    record_size = records_for_sstable_one_block[0].size
    block_size = random.randint(len(records_for_sstable_one_block) * record_size,
                                (len(records_for_sstable_one_block) + 1) * record_size)

    sstable_builder = SSTableBuilder(sstable_size=20000, block_size=150)
    for record in records_for_sstable_one_block:
        sstable_builder.add(record=record)

    sstable = sstable_builder.build(path=f"{TEST_SSTABLE_FIXTURES_DIRECTORY}/{file_name}.sst")

    assert len(sstable.meta_blocks) == 1
    assert sstable.meta_blocks[0].first_key == b'key1'
    assert sstable.meta_blocks[0].last_key == b'key3'

    yield sstable

    # Cleanup code (Delete the file created by the fixture)
    os.remove(sstable.file.path)


@pytest.fixture
def sstable_one_block_1(records_for_sstable_one_block):
    yield from build_sstable_one_block(records_for_sstable_one_block, "sstable_one_block")


@pytest.fixture
def sstable_one_block_2(records_for_sstable_one_block):
    yield from build_sstable_one_block(records_for_sstable_one_block, "sstable_one_block_2")


@pytest.fixture
def sstable_one_block_3(records_for_sstable_one_block):
    yield from build_sstable_one_block(records_for_sstable_one_block, "sstable_one_block_3")


@pytest.fixture
def sstable_one_block_4(records_for_sstable_one_block):
    yield from build_sstable_one_block(records_for_sstable_one_block, "sstable_one_block_4")


@pytest.fixture
def content_of_sstable_file_1():
    return b'this_is_the_content_of_sstable_file_1'


@pytest.fixture
def sstable_file_1(temporary_sstable_path, content_of_sstable_file_1):
    return SSTableFile.create(path=temporary_sstable_path, data=content_of_sstable_file_1)
