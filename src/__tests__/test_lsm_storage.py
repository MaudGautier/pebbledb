from unittest import mock

from src.bloom_filter import BloomFilter
from src.iterators import MergingIterator, SSTableIterator
from src.lsm_storage import LsmStorage
from src.__fixtures__.store import (
    store_with_multiple_immutable_memtables,
    store_with_multiple_immutable_memtables_records,
    store_with_duplicated_keys,
    store_with_duplicated_keys_records,
    store_with_one_sstable,
    store_with_multiple_l0_sstables,
    records_for_store_with_multiple_l0_sstables,
    TEST_DIRECTORY
)
from src.record import Record
from src.sstable import SSTable, SSTableFile


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
    nb_memtables = len(store.immutable_memtables)

    # WHEN
    store._flush_next_immutable_memtable()

    # THEN
    assert len(store.ss_tables) == 1  # One SSTable has been added
    assert len(store.immutable_memtables) == nb_memtables - 1  # One memtable has been removed

    # TODO: when iterator on sstable done, check that we have both key1 and key2 in there (or new test)


# TODO: add test to ensure that no freezing while flushing


def test_flush_memtables_prepends_sstables_in_l0_level(store_with_multiple_immutable_memtables,
                                                       store_with_multiple_immutable_memtables_records):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    nb_memtables = len(store.immutable_memtables)

    # WHEN
    store._flush_next_immutable_memtable()
    store._flush_next_immutable_memtable()

    # THEN
    assert len(store.ss_tables) == 2  # Two SSTables have been added
    assert len(store.immutable_memtables) == nb_memtables - 2  # Two memtables have been removed
    assert store.ss_tables[-1].first_key == store_with_multiple_immutable_memtables_records[0][0]
    assert store.ss_tables[-1].last_key == store_with_multiple_immutable_memtables_records[1][0]
    assert store.ss_tables[-2].first_key == store_with_multiple_immutable_memtables_records[2][0]
    assert store.ss_tables[-2].last_key == store_with_multiple_immutable_memtables_records[3][0]


def test_get_value_from_store(store_with_multiple_immutable_memtables):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    store._flush_next_immutable_memtable()

    # WHEN/THEN
    # From SSTable
    assert store.get(key="key1") == b'value1'
    assert store.get(key="key2") == b'value2'
    # From memTables
    assert store.get(key="key3") == b'value3'
    assert store.get(key="key4") == b'value4'
    assert store.get(key="key5") == b'value5'
    assert store.get(key="key6") == b'value6'


def test_dont_look_in_bloom_filter_if_key_absent():
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=2, nb_hash_functions=3)
    inserted_keys = ["foo", "bar"]
    for key in inserted_keys:
        bloom_filter.add(key=key)
    sstable = SSTable(meta_blocks=[],
                      meta_block_offset=0,
                      bloom_filter=bloom_filter,
                      file=SSTableFile(path=f"{TEST_DIRECTORY}/empty.sst", data=b''),
                      first_key="foo",
                      last_key="bar"
                      )
    store = LsmStorage()
    store.ss_tables.append(sstable)

    # WHEN/THEN
    for key in inserted_keys:
        with mock.patch.object(sstable, 'get', wraps=sstable.get) as mocked_sstable_get:
            # WHEN
            store.get(key=key)

            # THEN
            mocked_sstable_get.assert_called_once()

    with mock.patch.object(sstable, 'get', wraps=sstable.get) as mocked_sstable_get:
        # WHEN
        store.get("baz")

        # THEN
        mocked_sstable_get.assert_not_called()


def test_scan_on_both_memtables_and_sstables(store_with_one_sstable, store_with_multiple_immutable_memtables_records):
    # GIVEN
    store = store_with_one_sstable

    # WHEN
    scanned_records = list(record for record in store.scan(lower="key2", upper="key5"))

    # THEN
    expected_records = [Record(key=key, value=value) for key, value in store_with_multiple_immutable_memtables_records
                        if "key2" <= key <= "key5"]
    assert scanned_records == expected_records


def test_compact(store_with_multiple_l0_sstables, records_for_store_with_multiple_l0_sstables):
    # GIVEN
    store = store_with_multiple_l0_sstables

    l0_ss_table_iterator = MergingIterator(iterators=[
        SSTableIterator(sstable=sstable) for sstable in store.ss_tables
    ])

    # WHEN
    new_sstables = store._compact(sstables_iterator=l0_ss_table_iterator)

    # THEN
    assert len(new_sstables) == 2


def test_trigger_compaction(store_with_multiple_l0_sstables, records_for_store_with_multiple_l0_sstables):
    # GIVEN
    store = store_with_multiple_l0_sstables

    # WHEN
    store.trigger_compaction()

    # THEN
    assert len(store.ss_tables_levels) == 1
    assert len(store.ss_tables_levels[0]) == 2
    assert store.ss_tables_levels[0][0].first_key == "key1"
    assert store.ss_tables_levels[0][0].last_key == "key3"
    assert store.ss_tables_levels[0][1].first_key == "key4"
    assert store.ss_tables_levels[0][1].last_key == "key5"
    assert len(store.ss_tables) == 0
