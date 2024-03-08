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
