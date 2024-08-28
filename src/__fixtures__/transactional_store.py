import pytest

from src.__fixtures__.constants import TEST_DIRECTORY
from src.record import Record
from src.sequence_number_generator import SequenceNumberGenerator
from src.transactional_lsm_storage import TransactionalLsmStorage


@pytest.fixture
def empty_transactional_store():
    return TransactionalLsmStorage.create(directory=TEST_DIRECTORY)


@pytest.fixture
def key_values_for_transactional_store_with_duplicates_in_immutable_memtable():
    return [
        # Table 1
        (b'keyA', b'valueA'),
        (b'keyA', b'valueA'),
        # Table 2
        (b'keyA', b'valueA'),
        (b'keyB', b'valueB'),
        # Table 3
        (b'keyB', b'valueB'),
        (b'keyC', b'valueC'),
    ]


@pytest.fixture
def transactional_store_with_duplicates_in_immutable_memtable(
        key_values_for_transactional_store_with_duplicates_in_immutable_memtable):
    key_values = key_values_for_transactional_store_with_duplicates_in_immutable_memtable
    block_size = 2 * Record(key=key_values[0][0], value=key_values[0][1]).size
    SequenceNumberGenerator.reset()
    store = TransactionalLsmStorage.create(directory=TEST_DIRECTORY,
                                           max_sstable_size=block_size,
                                           block_size=block_size)

    for key, value in key_values:
        store.put(key=key, value=value)

    assert len(store.state.immutable_memtables) == 3
    assert store.state.immutable_memtables[0].get(key_values[5][0]) == key_values[5][1]
    assert store.state.immutable_memtables[0].get(key_values[4][0]) == key_values[4][1]
    assert store.state.immutable_memtables[1].get(key_values[3][0]) == key_values[3][1]
    assert store.state.immutable_memtables[1].get(key_values[2][0]) == key_values[2][1]
    assert store.state.immutable_memtables[2].get(key_values[1][0]) == key_values[1][1]
    assert store.state.immutable_memtables[2].get(key_values[0][0]) == key_values[0][1]

    return store
