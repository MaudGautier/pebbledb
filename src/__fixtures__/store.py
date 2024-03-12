import pytest

from src.lsm_storage import LsmStorage


@pytest.fixture
def store_with_multiple_immutable_memtables_records():
    return [
        ("key1", b'value1'),
        ("key2", b'value2'),
        ("key3", b'value3'),
        ("key4", b'value4'),
        ("key5", b'value5'),
        ("key6", b'value6'),
    ]


@pytest.fixture
def store_with_multiple_immutable_memtables(store_with_multiple_immutable_memtables_records):
    store = LsmStorage(max_sstable_size=30)
    for record in store_with_multiple_immutable_memtables_records:
        store.put(key=record[0], value=record[1])

    assert len(store.immutable_memtables) == 3

    return store
