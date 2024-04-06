import pytest

from src.lsm_storage import LsmStorage

TEST_DIRECTORY = "./test_store"


@pytest.fixture
def store_with_multiple_immutable_memtables_records():
    return [
        # Table 1
        ("key1", b'value1'),
        ("key2", b'value2'),
        # Table 2
        ("key3", b'value3'),
        ("key4", b'value4'),
        # Table 3
        ("key5", b'value5'),
        ("key6", b'value6'),
    ]


@pytest.fixture
def store_with_multiple_immutable_memtables(store_with_multiple_immutable_memtables_records):
    store = LsmStorage(max_sstable_size=30, block_size=20, directory=TEST_DIRECTORY)
    for record in store_with_multiple_immutable_memtables_records:
        store.put(key=record[0], value=record[1])

    assert len(store.immutable_memtables) == 3

    return store


@pytest.fixture
def store_with_duplicated_keys_records():
    return [
        ("key1", b'value1A'),
        ("key2", b'value2A'),
        ("key3", b'value3'),
        ("key1", b'value1B'),
        ("key1", b'value1C'),
        ("key2", b'value2B'),
        ("key1", b'value1D'),
        ("key4", b'value4A'),
    ]


@pytest.fixture
def store_with_duplicated_keys(store_with_duplicated_keys_records):
    store = LsmStorage(max_sstable_size=30)
    for record in store_with_duplicated_keys_records:
        store.put(key=record[0], value=record[1])

    assert len(store.immutable_memtables) == 4

    return store


@pytest.fixture
def store_with_one_sstable(store_with_multiple_immutable_memtables_records):
    store = LsmStorage(max_sstable_size=30, block_size=20, directory=TEST_DIRECTORY)
    for record in store_with_multiple_immutable_memtables_records:
        store.put(key=record[0], value=record[1])
    store._flush_next_immutable_memtable()

    assert len(store.immutable_memtables) == 2
    assert len(store.ss_tables) == 1

    return store


@pytest.fixture
def records_for_store_with_multiple_l0_sstables():
    return [
        # Table 0
        ("key1", b'first_value1'),
        ("key2", b'value2'),
        # Table 1
        ("key3", b'first_value3'),
        ("key1", b'second_value1'),
        # Table 2
        ("key3", b'value3'),
        ("key4", b'value4'),
        # Table 3
        ("key5", b'value5'),
        ("key1", b'value1'),
    ]


@pytest.fixture
def store_with_multiple_l0_sstables(records_for_store_with_multiple_l0_sstables):
    store = LsmStorage(max_sstable_size=35, block_size=30, directory=TEST_DIRECTORY)
    for record in records_for_store_with_multiple_l0_sstables:
        store.put(key=record[0], value=record[1])
    assert len(store.immutable_memtables) == 4
    for i in range(len(store.immutable_memtables)):
        store._flush_next_immutable_memtable()

    assert len(store.immutable_memtables) == 0
    assert len(store.ss_tables) == 4

    return store
