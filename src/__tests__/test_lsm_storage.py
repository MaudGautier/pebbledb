from unittest import mock

from src.lsm_storage import LsmStorage


def test_can_read_a_value_inserted():
    # GIVEN
    store = LsmStorage()

    # WHEN
    store.put(key="key", value=b'value')
    store.put(key="key2", value=b'value2')

    # THEN
    assert store.get(key="key") == b'value'
    assert store.get(key="key2") == b'value2'


def test_try_freeze():
    # GIVEN
    store = LsmStorage(max_sstable_size=50)

    # WHEN/THEN
    with mock.patch.object(store, '_freeze_memtable', wraps=store._freeze_memtable) as mocked_freeze:
        # WHEN
        store._try_freeze()

        # THEN
        mocked_freeze.assert_not_called()

    with mock.patch.object(store, '_freeze_memtable', wraps=store._freeze_memtable) as mocked_freeze:
        # WHEN
        store.put(key="a_short_key", value=b'a_short_value')

        # THEN
        mocked_freeze.assert_not_called()

    with mock.patch.object(store, '_freeze_memtable', wraps=store._freeze_memtable) as mocked_freeze:
        # WHEN
        store.put(key="a_veeeeeeryyyyyyy_loooong_key", value=b'a_veeeeeeryyyyyyy_loooong_value')

        # THEN
        mocked_freeze.assert_called_once()


def test_freeze_memtable():
    # GIVEN
    store = LsmStorage(max_sstable_size=1000)
    store.put(key="key1", value=b'value1')
    store.put(key="key2", value=b'value2')
    store.put(key="key3", value=b'value3')
    assert len(store.immutable_memtables) == 0
    assert store.memtable.get("key1") == b'value1'
    assert store.memtable.get("key2") == b'value2'
    assert store.memtable.get("key3") == b'value3'

    # WHEN
    store._freeze_memtable()

    # THEN
    assert len(store.immutable_memtables) == 1
    assert store.memtable.get("key1") is None
    assert store.memtable.get("key2") is None
    assert store.memtable.get("key3") is None
    assert store.immutable_memtables[0].get("key1") == b'value1'
    assert store.immutable_memtables[0].get("key2") == b'value2'
    assert store.immutable_memtables[0].get("key3") == b'value3'


