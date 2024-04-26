import random

import pytest

from src.blocks import DataBlockBuilder
from src.record import Record


@pytest.fixture
def records_for_data_block_with_duplicates():
    return [
        Record(key=b'key1', value=b'value1'),
        Record(key=b'key1', value=b'value2'),
        Record(key=b'key3', value=b'value3'),
        Record(key=b'key3', value=b'value4'),
        Record(key=b'key5', value=b'value5'),
        Record(key=b'key5', value=b'value6'),
        Record(key=b'key7', value=b'value7'),
        Record(key=b'key7', value=b'value8'),
    ]


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
