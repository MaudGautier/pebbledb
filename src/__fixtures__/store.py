import os
import time

import pytest

from src.__fixtures__.constants import TEST_DIRECTORY
from src.lsm_storage import LsmStorage


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
    store = LsmStorage.create(max_sstable_size=30, block_size=20, directory=TEST_DIRECTORY)
    for record in store_with_multiple_immutable_memtables_records:
        store.put(key=record[0], value=record[1])

    assert len(store.state.immutable_memtables) == 3

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
    store = LsmStorage.create(max_sstable_size=30, directory=TEST_DIRECTORY)
    for record in store_with_duplicated_keys_records:
        store.put(key=record[0], value=record[1])

    assert len(store.state.immutable_memtables) == 4

    return store


@pytest.fixture
def store_with_one_l0_sstable(store_with_multiple_immutable_memtables_records):
    store = LsmStorage.create(max_sstable_size=30, block_size=20, directory=TEST_DIRECTORY)
    for record in store_with_multiple_immutable_memtables_records:
        store.put(key=record[0], value=record[1])
    store.flush_next_immutable_memtable()

    assert len(store.state.immutable_memtables) == 2
    assert len(store.state.sstables_level0) == 1

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
    store = LsmStorage.create(max_sstable_size=35, block_size=30, directory=TEST_DIRECTORY)
    for record in records_for_store_with_multiple_l0_sstables:
        store.put(key=record[0], value=record[1])
    assert len(store.state.immutable_memtables) == 4
    for i in range(len(store.state.immutable_memtables)):
        store.flush_next_immutable_memtable()

    assert len(store.state.immutable_memtables) == 0
    assert len(store.state.sstables_level0) == 4

    return store


@pytest.fixture
def records_for_store_with_multiple_l1_sstables():
    return [
        # Table 0
        ("key4", b'value4'),
        ("key3", b'value3'),
        # Table 1
        ("key8", b'value8'),
        ("key5", b'value5'),
        # Table 2
        ("key1", b'value1'),
        ("key6", b'value6'),
        # Table 3
        ("key7", b'value7'),
        ("key2", b'value2'),
    ]


@pytest.fixture
def store_with_multiple_l1_sstables(records_for_store_with_multiple_l1_sstables):
    nb_levels = 2
    store = LsmStorage.create(max_sstable_size=20, block_size=20, directory=TEST_DIRECTORY, nb_levels=nb_levels)
    for record in records_for_store_with_multiple_l1_sstables:
        store.put(key=record[0], value=record[1])
    assert len(store.state.immutable_memtables) == 4
    for i in range(len(store.state.immutable_memtables)):
        store.flush_next_immutable_memtable()

    assert len(store.state.immutable_memtables) == 0
    assert len(store.state.sstables_level0) == 4

    store.force_compaction_l0()

    assert len(store.state.sstables_level0) == 0
    assert len(store.state.sstables_levels) == nb_levels
    assert len(store.state.sstables_levels[0]) == 4

    return store


@pytest.fixture
def records_for_store_with_four_l1_and_one_l2_sstables():
    return [
        # Table 0
        ("key4", b'value4'),
        ("key3", b'value3'),
        # Table 1
        ("key8", b'value8'),
        ("key5", b'value5'),
        # Table 2
        ("key1", b'value1'),
        ("key6", b'value6'),
        # Table 3
        ("key7", b'value7'),
        ("key2", b'value2'),
        # Table 4
        ("key9", b'value9'),
        ("key10", b'value10'),
    ]


@pytest.fixture
def store_with_four_l1_and_one_l2_sstables(records_for_store_with_four_l1_and_one_l2_sstables):
    nb_levels = 2
    store = LsmStorage.create(max_sstable_size=20, block_size=20, directory=TEST_DIRECTORY, nb_levels=nb_levels)
    for record in records_for_store_with_four_l1_and_one_l2_sstables:
        store.put(key=record[0], value=record[1])
    assert len(store.state.immutable_memtables) == 5
    for i in range(4):
        store.flush_next_immutable_memtable()

    assert len(store.state.immutable_memtables) == 1
    assert len(store.state.sstables_level0) == 4

    store.force_compaction_l0()
    store.force_compaction_l1_or_more_level(level=1)

    assert len(store.state.sstables_level0) == 0
    assert len(store.state.sstables_levels) == 2
    assert len(store.state.sstables_levels[0]) == 0
    assert len(store.state.sstables_levels[1]) == 4

    store.flush_next_immutable_memtable()

    assert len(store.state.sstables_level0) == 1
    assert len(store.state.sstables_levels) == nb_levels
    assert len(store.state.sstables_levels[0]) == 0
    assert len(store.state.sstables_levels[1]) == 4

    store.force_compaction_l0()

    assert len(store.state.sstables_level0) == 0
    assert len(store.state.sstables_levels) == 2
    assert len(store.state.sstables_levels[0]) == 1
    assert len(store.state.sstables_levels[1]) == 4

    return store


@pytest.fixture
def records_for_store_with_one_sstable_at_five_levels():
    return [
        # Table 0
        ("keyA", b'valueA'),
        ("keyB", b'valueB'),
        # Table 1
        ("keyC", b'valueC'),
        ("keyD", b'valueD'),
        # Table 2
        ("keyE", b'valueE'),
        ("keyF", b'valueF'),
        # Table 3
        ("keyG", b'valueG'),
        ("keyH", b'valueH'),
        # Table 4
        ("keyI", b'valueI'),
        ("keyJ", b'valueJ'),
    ]


@pytest.fixture
def store_with_one_sstable_at_five_levels(records_for_store_with_one_sstable_at_five_levels):
    nb_levels = 4
    store = LsmStorage.create(max_sstable_size=20, block_size=20, directory=TEST_DIRECTORY, nb_levels=nb_levels)
    store._configuration.levels_ratio = 10  # High value (that makes no sense) to build the target mocked store
    for record in records_for_store_with_one_sstable_at_five_levels:
        store.put(key=record[0], value=record[1])

    # Table 0
    store.flush_next_immutable_memtable()
    store.force_compaction_l0()
    store.force_compaction_l1_or_more_level(level=1)
    store.force_compaction_l1_or_more_level(level=2)
    store.force_compaction_l1_or_more_level(level=3)

    # Table 1
    store.flush_next_immutable_memtable()
    store.force_compaction_l0()
    store.force_compaction_l1_or_more_level(level=1)
    store.force_compaction_l1_or_more_level(level=2)

    # Table 2
    store.flush_next_immutable_memtable()
    store.force_compaction_l0()
    store.force_compaction_l1_or_more_level(level=1)

    # Table 3
    store.flush_next_immutable_memtable()
    store.force_compaction_l0()

    # Table 4
    store.flush_next_immutable_memtable()

    # Assertions
    assert len(store.state.sstables_level0) == 1
    assert len(store.state.sstables_levels) == nb_levels
    assert len(store.state.sstables_levels[0]) == 1
    assert len(store.state.sstables_levels[1]) == 1
    assert len(store.state.sstables_levels[2]) == 1
    assert len(store.state.sstables_levels[3]) == 1

    return store


@pytest.fixture
def empty_store():
    return LsmStorage.create(directory=TEST_DIRECTORY)
