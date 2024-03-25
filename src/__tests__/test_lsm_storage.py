from unittest import mock

from src.lsm_storage import LsmStorage
from src.__fixtures__.store import (
    store_with_multiple_immutable_memtables,
    store_with_multiple_immutable_memtables_records,
    store_with_duplicated_keys,
    store_with_duplicated_keys_records
)
from src.record import Record
from src.sstable import SSTable


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


def test_retrieve_value_from_old_memtable(
        store_with_multiple_immutable_memtables,
        store_with_multiple_immutable_memtables_records):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    missing_key = "key7"
    assert missing_key not in list(zip(*store_with_multiple_immutable_memtables_records))[0]

    # WHEN/THEN
    values = [store.get(key=record[0]) for record in store_with_multiple_immutable_memtables_records]
    value_for_missing_key = store.get(key=missing_key)

    # THEN
    expected_values = [record[1] for record in store_with_multiple_immutable_memtables_records]
    assert values == expected_values
    assert value_for_missing_key is None


def test_scan(store_with_multiple_immutable_memtables,
              store_with_multiple_immutable_memtables_records):
    # GIVEN
    store = store_with_multiple_immutable_memtables

    # WHEN
    records = list(store.scan(lower="key1", upper="key4"))

    # THEN
    expected_records = [Record(key=key, value=value) for key, value in store_with_multiple_immutable_memtables_records
                        if "key1" <= key <= "key4"]
    assert records == expected_records


def test_scan_when_duplicates(
        store_with_duplicated_keys,
        store_with_duplicated_keys_records):
    """Tests that the most recent value is selected for each key in the range"""
    # GIVEN
    store = store_with_duplicated_keys

    # WHEN
    records = list(store.scan(lower="key1", upper="key3"))

    # THEN
    expected_records = []
    seen_keys = set()
    for key, value in store_with_duplicated_keys_records[::-1]:
        if key in seen_keys:
            continue
        if not ("key1" <= key <= "key3"):
            continue
        seen_keys.add(key)
        expected_records.append(Record(key=key, value=value))

    expected_records.sort()
    assert records == expected_records


def test_flush_next_immutable_memtable(store_with_multiple_immutable_memtables):
    # GIVEN
    store = store_with_multiple_immutable_memtables

    # WHEN
    store.flush_next_immutable_memtable()

    # THEN
    assert len(store.ss_tables_paths) == 1  # One SSTable has been added

    # TODO: when iterator on sstable done, check that we have both key1 and key2 in there (or new test)
