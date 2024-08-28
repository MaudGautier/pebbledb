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
        (b'keyA', b'valueA1'),
        (b'keyA', b'valueA2'),
        # Table 2
        (b'keyA', b'valueA3'),
        (b'keyB', b'valueB1'),
        # Table 3
        (b'keyB', b'valueB2'),
        (b'keyC', b'valueC1'),
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
    assert store.state.immutable_memtables[0].map.get(key_values[5][0])[0].endswith(key_values[5][1])
    assert store.state.immutable_memtables[0].map.get(key_values[4][0])[0].endswith(key_values[4][1])
    assert store.state.immutable_memtables[1].map.get(key_values[3][0])[0].endswith(key_values[3][1])
    assert store.state.immutable_memtables[1].map.get(key_values[2][0])[0].endswith(key_values[2][1])
    assert store.state.immutable_memtables[2].map.get(key_values[1][0])[1].endswith(key_values[1][1])
    assert store.state.immutable_memtables[2].map.get(key_values[0][0])[0].endswith(key_values[0][1])

    return store


@pytest.fixture
def key_values_for_transactional_store_with_duplicates_in_l0_sstables():
    return [
        # Table 1
        (b'keyA', b'valueA1'),
        (b'keyA', b'valueA2'),
        # Table 2
        (b'keyA', b'valueA3'),
        (b'keyB', b'valueB1'),
        # Table 3
        (b'keyB', b'valueB2'),
        (b'keyC', b'valueC1'),
    ]


@pytest.fixture
def transactional_store_with_duplicates_in_l0_sstables(
        key_values_for_transactional_store_with_duplicates_in_l0_sstables):
    key_values = key_values_for_transactional_store_with_duplicates_in_l0_sstables
    SequenceNumberGenerator.reset()
    block_size = 2 * Record(key=key_values[0][0], value=key_values[0][1]).size
    store = TransactionalLsmStorage.create(directory=TEST_DIRECTORY,
                                           max_sstable_size=block_size,
                                           block_size=block_size)

    for key, value in key_values:
        store.put(key=key, value=value)
    for i in range(len(store.state.immutable_memtables)):
        store._trigger_flush()

    assert len(store.state.immutable_memtables) == 0
    assert len(store.state.sstables_level0) == 3
    assert store.state.sstables_level0[0].get(key_values[5][0]) == key_values[5][1]
    assert store.state.sstables_level0[0].get(key_values[4][0]) == key_values[4][1]
    assert store.state.sstables_level0[1].get(key_values[3][0]) == key_values[3][1]
    assert store.state.sstables_level0[1].get(key_values[2][0]) == key_values[2][1]
    assert store.state.sstables_level0[2].get(key_values[1][0]) == key_values[1][1]
    assert key_values[0][1] in store.state.sstables_level0[2].read_data_block(0).data

    return store


@pytest.fixture
def key_values_for_transactional_store_with_duplicates_in_l1_sstables():
    return [
        # Table 1
        (b'keyA', b'valueA1'),
        (b'keyA', b'valueA2'),
        # Table 2
        (b'keyA', b'valueA3'),
        (b'keyB', b'valueB1'),
        # Table 3
        (b'keyB', b'valueB2'),
        (b'keyC', b'valueC1'),
    ]


@pytest.fixture
def transactional_store_with_duplicates_in_l1_sstables(
        key_values_for_transactional_store_with_duplicates_in_l1_sstables):
    key_values = key_values_for_transactional_store_with_duplicates_in_l1_sstables
    SequenceNumberGenerator.reset()
    block_size = 2 * Record(key=key_values[0][0], value=key_values[0][1]).size
    store = TransactionalLsmStorage.create(directory=TEST_DIRECTORY,
                                           max_sstable_size=block_size,
                                           block_size=block_size)

    for key, value in key_values:
        store.put(key=key, value=value)
    for i in range(len(store.state.immutable_memtables)):
        store._trigger_flush()
    store.state.sstables_levels[0] = store.state.sstables_level0
    store.state.sstables_level0 = []

    assert len(store.state.immutable_memtables) == 0
    assert len(store.state.sstables_level0) == 0
    assert len(store.state.sstables_levels[0]) == 3
    assert store.state.sstables_levels[0][0].get(key_values[5][0]) == key_values[5][1]
    assert store.state.sstables_levels[0][0].get(key_values[4][0]) == key_values[4][1]
    assert store.state.sstables_levels[0][1].get(key_values[3][0]) == key_values[3][1]
    assert store.state.sstables_levels[0][1].get(key_values[2][0]) == key_values[2][1]
    assert store.state.sstables_levels[0][2].get(key_values[1][0]) == key_values[1][1]
    assert key_values[0][1] in store.state.sstables_levels[0][2].read_data_block(0).data

    return store
