import random

import pytest

from src.blocks import DataBlockBuilder
from src.record import Record


@pytest.fixture
def records_key1_for_data_block_with_duplicates():
    return [
        Record(key=b'key1', value=b'value1A'),
        Record(key=b'key1', value=b'value1B'),
        Record(key=b'key1', value=b'value1C'),
    ]


@pytest.fixture
def records_key3_for_data_block_with_duplicates():
    return [
        Record(key=b'key3', value=b'value3A'),
        Record(key=b'key3', value=b'value3B'),
    ]


@pytest.fixture
def records_key5_for_data_block_with_duplicates():
    return [
        Record(key=b'key5', value=b'value5A'),
        Record(key=b'key5', value=b'value5B'),
    ]


@pytest.fixture
def records_key7_for_data_block_with_duplicates():
    return [
        Record(key=b'key7', value=b'value7A'),
        Record(key=b'key7', value=b'value7B'),
        Record(key=b'key7', value=b'value7C'),
    ]


@pytest.fixture
def records_for_data_block_with_duplicates(records_key1_for_data_block_with_duplicates,
                                           records_key3_for_data_block_with_duplicates,
                                           records_key5_for_data_block_with_duplicates,
                                           records_key7_for_data_block_with_duplicates):
    records_key1 = records_key1_for_data_block_with_duplicates
    records_key3 = records_key3_for_data_block_with_duplicates
    records_key5 = records_key5_for_data_block_with_duplicates
    records_key7 = records_key7_for_data_block_with_duplicates

    # In reverse because they are written from most to least recent in data blocks
    return (list(reversed(records_key1)) +
            list(reversed(records_key3)) +
            list(reversed(records_key5)) +
            list(reversed(records_key7)))


@pytest.fixture
def data_block_with_duplicates(records_for_data_block_with_duplicates):
    record_size = records_for_data_block_with_duplicates[0].size
    block_size = random.randint(len(records_for_data_block_with_duplicates) * record_size,
                                # Upper limit does not matter
                                (len(records_for_data_block_with_duplicates) + 1) * record_size)
    block_builder = DataBlockBuilder(target_size=block_size)

    for record in records_for_data_block_with_duplicates:
        block_builder.add(record=record)

    block = block_builder.create_block()

    return block
