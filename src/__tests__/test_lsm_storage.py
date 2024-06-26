import threading
import time
from unittest import mock

from src.bloom_filter import BloomFilter
from src.iterators import MergingIterator, SSTableIterator
from src.lsm_storage import LsmStorage
from src.manifest import CompactionEvent, FlushEvent
from src.record import Record
from src.sstable import SSTable, SSTableFile


def test_can_read_a_value_inserted(empty_store):
    # GIVEN
    store = empty_store

    # WHEN
    store.put(key="key", value=b'value')
    store.put(key="key2", value=b'value2')

    # THEN
    assert store.get(key="key") == b'value'
    assert store.get(key="key2") == b'value2'


def test_try_freeze(empty_store):
    # GIVEN
    store = empty_store
    store._configuration.max_sstable_size = 50

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


def test_freeze_memtable(empty_store):
    # GIVEN
    store = empty_store
    store.max_sstable_size = 1000
    store.put(key="key1", value=b'value1')
    store.put(key="key2", value=b'value2')
    store.put(key="key3", value=b'value3')
    assert len(store.state.immutable_memtables) == 0
    assert store.state.memtable.get("key1") == b'value1'
    assert store.state.memtable.get("key2") == b'value2'
    assert store.state.memtable.get("key3") == b'value3'

    # WHEN
    store._freeze_memtable()

    # THEN
    assert len(store.state.immutable_memtables) == 1
    assert store.state.memtable.get("key1") is None
    assert store.state.memtable.get("key2") is None
    assert store.state.memtable.get("key3") is None
    assert store.state.immutable_memtables[0].get("key1") == b'value1'
    assert store.state.immutable_memtables[0].get("key2") == b'value2'
    assert store.state.immutable_memtables[0].get("key3") == b'value3'


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
    nb_memtables = len(store.state.immutable_memtables)

    # WHEN
    store.flush_next_immutable_memtable()

    # THEN
    assert len(store.state.sstables_level0) == 1  # One SSTable has been added
    assert len(store.state.immutable_memtables) == nb_memtables - 1  # One memtable has been removed

    # TODO: when iterator on sstable done, check that we have both key1 and key2 in there (or new test)


def test_flush_waits_for_freeze(empty_store):
    # GIVEN
    storage = empty_store
    storage.put("key", b'value')
    storage.state.memtable.approximate_size = storage._configuration.max_sstable_size + 1

    # Original methods with timing
    times = {}

    # Wrap _do_flush and _do_freeze with timing
    original_do_flush = storage._do_flush
    original_freeze = storage._freeze_memtable

    def _do_flush_with_timing():
        times['flush_start'] = time.time()
        original_do_flush()
        times['flush_end'] = time.time()

    def _freeze_with_timing():
        times['freeze_start'] = time.time()
        original_freeze()
        times['freeze_end'] = time.time()

    # Replace original methods with timed methods
    storage._do_flush = _do_flush_with_timing
    storage._freeze_memtable = _freeze_with_timing

    # WHEN
    # Start threads
    freeze_thread = threading.Thread(target=storage._try_freeze())
    flush_thread = threading.Thread(target=storage.flush_next_immutable_memtable())

    freeze_thread.start()
    flush_thread.start()

    freeze_thread.join()
    flush_thread.join()

    # THEN
    # Assert that freeze ended before flush started
    assert times['freeze_end'] < times['flush_start']


def test_freeze_waits_for_flush(empty_store, empty_memtable):
    # GIVEN
    storage = empty_store
    storage.state.memtable.approximate_size = storage._configuration.max_sstable_size + 1
    memtable_to_flush = empty_memtable
    memtable_to_flush.put("key", b'value')
    storage.state.immutable_memtables.append(memtable_to_flush)

    # Original methods with timing
    times = {}

    original_freeze_memtable = storage._freeze_memtable
    original_try_compact = storage._try_compact

    def _freeze_memtable_with_timing():
        times['freeze_memtable_start'] = time.time()
        original_freeze_memtable()  # Ensure this calls the real implementation
        times['freeze_memtable_end'] = time.time()

    # _try_compact is called outside the mutex lock => adding times around its execution so that we can track that the
    # beginning of freeze starts after the end of the flush under lock (i.e. the beginning of _try_compact)
    def _try_compact_with_timing():
        times['try_compact_start'] = time.time()
        original_try_compact()  # Ensures this calls the real implementation
        times['try_compact_end'] = time.time()

    def flush_next_immutable_memtable_with_timing():
        times['flush_start'] = time.time()
        storage.flush_next_immutable_memtable()
        times['flush_end'] = time.time()

    def _try_freeze_with_timing():
        times['freeze_start'] = time.time()
        storage._try_freeze()
        times['freeze_end'] = time.time()

    # Replace or wrap the original _freeze_memtable method with the timing version
    storage._freeze_memtable = _freeze_memtable_with_timing
    storage._try_compact = _try_compact_with_timing  # Do nothing in try compact

    # WHEN
    # Start threads
    flush_thread = threading.Thread(target=flush_next_immutable_memtable_with_timing)
    freeze_thread = threading.Thread(target=_try_freeze_with_timing)

    flush_thread.start()
    freeze_thread.start()

    flush_thread.join()
    freeze_thread.join()

    # THEN
    # Assert that the portion of flush under locking (everything before compact) ended before freeze memtable starts
    assert times['try_compact_start'] < times['freeze_memtable_start']
    # Note: assert times['flush_end'] < times['freeze_memtable_start'] fails because the last part of the flush
    # (_try_compact) is not under a mutex lock


def test_flush_memtables_prepends_sstables_in_l0_level(store_with_multiple_immutable_memtables,
                                                       store_with_multiple_immutable_memtables_records):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    nb_memtables = len(store.state.immutable_memtables)

    # WHEN
    store.flush_next_immutable_memtable()
    store.flush_next_immutable_memtable()

    # THEN
    assert len(store.state.sstables_level0) == 2  # Two SSTables have been added
    assert len(store.state.immutable_memtables) == nb_memtables - 2  # Two memtables have been removed
    assert store.state.sstables_level0[-1].first_key == store_with_multiple_immutable_memtables_records[0][0]
    assert store.state.sstables_level0[-1].last_key == store_with_multiple_immutable_memtables_records[1][0]
    assert store.state.sstables_level0[-2].first_key == store_with_multiple_immutable_memtables_records[2][0]
    assert store.state.sstables_level0[-2].last_key == store_with_multiple_immutable_memtables_records[3][0]


def test_get_value_from_store(store_with_multiple_immutable_memtables):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    store.flush_next_immutable_memtable()

    # WHEN/THEN
    # From SSTable
    assert store.get(key="key1") == b'value1'
    assert store.get(key="key2") == b'value2'
    # From memTables
    assert store.get(key="key3") == b'value3'
    assert store.get(key="key4") == b'value4'
    assert store.get(key="key5") == b'value5'
    assert store.get(key="key6") == b'value6'


def test_dont_look_in_bloom_filter_if_key_absent(temporary_sstable_path, empty_store):
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=2, nb_hash_functions=3)
    inserted_keys = ["foo", "bar"]
    for key in inserted_keys:
        bloom_filter.add(key=key)
    sstable = SSTable(meta_blocks=[],
                      meta_block_offset=0,
                      bloom_filter=bloom_filter,
                      file=SSTableFile.create(path=temporary_sstable_path, data=b''),
                      first_key="foo",
                      last_key="bar"
                      )
    store = empty_store
    store.state.sstables_level0.append(sstable)

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


def test_scan_on_both_memtables_and_sstables(store_with_one_l0_sstable,
                                             store_with_multiple_immutable_memtables_records):
    # GIVEN
    store = store_with_one_l0_sstable

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
        SSTableIterator(sstable=sstable) for sstable in store.state.sstables_level0
    ])

    # WHEN
    new_sstables = store._compact(records_iterator=l0_ss_table_iterator)

    # THEN
    assert len(new_sstables) == 2


def test_trigger_l0_compaction(store_with_multiple_l0_sstables, records_for_store_with_multiple_l0_sstables):
    # GIVEN
    store = store_with_multiple_l0_sstables

    # WHEN
    store.force_compaction_l0()

    # THEN
    assert len(store.state.sstables_levels) == store._configuration.nb_levels
    assert len(store.state.sstables_levels[0]) == 2
    assert store.state.sstables_levels[0][0].first_key == "key1"
    assert store.state.sstables_levels[0][0].last_key == "key3"
    assert store.state.sstables_levels[0][1].first_key == "key4"
    assert store.state.sstables_levels[0][1].last_key == "key5"
    assert len(store.state.sstables_level0) == 0


def test_read_path_with_sstables_on_levels_0_and_1(store_with_multiple_l1_sstables,
                                                   records_for_store_with_multiple_l1_sstables):
    # GIVEN
    store = store_with_multiple_l1_sstables

    # WHEN
    values = [store.get(key=record[0]) for record in records_for_store_with_multiple_l1_sstables]

    # THEN
    expected_values = [record[1] for record in records_for_store_with_multiple_l1_sstables]
    assert values == expected_values


def test_trigger_l1_compaction_to_l2(store_with_multiple_l1_sstables, records_for_store_with_multiple_l1_sstables):
    # GIVEN
    store = store_with_multiple_l1_sstables

    # WHEN
    store.force_compaction_l1_or_more_level(level=1)

    # THEN
    assert len(store.state.sstables_levels) == store._configuration.nb_levels
    assert len(store.state.sstables_levels[0]) == 0
    assert len(store.state.sstables_levels[1]) == 4
    assert store.state.sstables_levels[1][0].first_key == "key1"
    assert store.state.sstables_levels[1][0].last_key == "key2"
    assert store.state.sstables_levels[1][1].first_key == "key3"
    assert store.state.sstables_levels[1][1].last_key == "key4"
    assert store.state.sstables_levels[1][2].first_key == "key5"
    assert store.state.sstables_levels[1][2].last_key == "key6"
    assert store.state.sstables_levels[1][3].first_key == "key7"
    assert store.state.sstables_levels[1][3].last_key == "key8"


def test_try_compact_should_force_compact_l0_if_above_the_threshold(store_with_one_l0_sstable):
    # GIVEN
    store = store_with_one_l0_sstable
    store._configuration.max_l0_sstables = 1

    # WHEN/THEN
    with mock.patch.object(store, 'force_compaction_l0', wraps=store.force_compaction_l0) as mocked_compact:
        # WHEN
        store._try_compact()

        # THEN
        mocked_compact.assert_called_once()


def test_try_compact_should_not_force_compact_l0_if_below_the_threshold(store_with_one_l0_sstable):
    # GIVEN
    store = store_with_one_l0_sstable
    store._configuration.max_l0_sstables = 2

    # WHEN/THEN
    with mock.patch.object(store, 'force_compaction_l0', wraps=store.force_compaction_l0) as mocked_compact:
        # WHEN
        store._try_compact()

        # THEN
        mocked_compact.assert_not_called()


def test_try_compact_should_force_compact_l1_if_above_the_threshold(store_with_four_l1_and_one_l2_sstables):
    # GIVEN
    store = store_with_four_l1_and_one_l2_sstables
    store._configuration.levels_ratio = 0.2

    # WHEN/THEN
    with mock.patch.object(store, 'force_compaction_l1_or_more_level',
                           wraps=store.force_compaction_l1_or_more_level) as mocked_compact:
        # WHEN
        store._try_compact()

        # THEN
        mocked_compact.assert_called_once()


def test_try_compact_should_not_force_compact_l1_if_below_the_threshold(store_with_four_l1_and_one_l2_sstables):
    # GIVEN
    store = store_with_four_l1_and_one_l2_sstables
    store._configuration.levels_ratio = 0.3

    # WHEN/THEN
    with mock.patch.object(store, 'force_compaction_l1_or_more_level',
                           wraps=store.force_compaction_l1_or_more_level) as mocked_compact:
        # WHEN
        store._try_compact()

        # THEN
        mocked_compact.assert_not_called()


def test_try_compact_should_compact_in_cascade(store_with_one_sstable_at_five_levels):
    # GIVEN
    store = store_with_one_sstable_at_five_levels
    store._configuration.levels_ratio = 2
    store._configuration.max_l0_sstables = 1

    # WHEN/THEN
    with mock.patch.object(store, 'force_compaction_l1_or_more_level',
                           wraps=store.force_compaction_l1_or_more_level) as mocked_compact:
        # WHEN
        store._try_compact()

        # THEN
        mocked_compact.assert_has_calls([mock.call(level=1), mock.call(level=2), mock.call(level=3)])
        assert len(store.state.sstables_levels[0]) == 0
        assert len(store.state.sstables_levels[1]) == 0
        assert len(store.state.sstables_levels[2]) == 0
        assert len(store.state.sstables_levels[3]) == 5


def test_try_compact_should_not_compact_an_empty_level(store_with_one_sstable_at_last_level):
    # GIVEN
    store = store_with_one_sstable_at_last_level
    store._configuration.levels_ratio = 2
    store._configuration.max_l0_sstables = 10

    # WHEN/THEN
    with mock.patch.object(store, 'force_compaction_l1_or_more_level',
                           wraps=store.force_compaction_l1_or_more_level) as mocked_compact:
        # WHEN
        store._try_compact()

        # THEN
        mocked_compact.assert_not_called()
        assert len(store.state.sstables_levels[0]) == 0
        assert len(store.state.sstables_levels[1]) == 0
        assert len(store.state.sstables_levels[2]) == 1


def test_flush_next_immutable_memtable_tries_compacting(store_with_multiple_immutable_memtables):
    # GIVEN
    store = store_with_multiple_immutable_memtables

    # WHEN/THEN
    with mock.patch.object(store, '_try_compact', wraps=store._try_compact) as mocked_compact:
        # WHEN
        store.flush_next_immutable_memtable()

        # THEN
        mocked_compact.assert_called_once()


def test_wal_associated_to_flushed_memtable_gets_deleted_upon_flush(store_with_multiple_immutable_memtables):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    next_memtable_wal = store.state.immutable_memtables[-1].wal

    # WHEN/THEN
    with mock.patch.object(next_memtable_wal, 'remove_self') as mocked_remove_self:
        # WHEN
        store.flush_next_immutable_memtable()

        # THEN
        mocked_remove_self.assert_called_once()


def test_reconstruct_from_manifest_has_same_configuration(sample_manifest_0_without_events,
                                                          configuration_for_sample_manifest_0):
    # GIVEN
    manifest = sample_manifest_0_without_events

    # WHEN
    reconstructed_store = LsmStorage.reconstruct_from_manifest(manifest_path=manifest.file.path)

    # THEN
    assert reconstructed_store._configuration.nb_levels == configuration_for_sample_manifest_0.nb_levels
    assert reconstructed_store._configuration.levels_ratio == configuration_for_sample_manifest_0.levels_ratio
    assert reconstructed_store._configuration.max_l0_sstables == configuration_for_sample_manifest_0.max_l0_sstables
    assert reconstructed_store._configuration.max_sstable_size == configuration_for_sample_manifest_0.max_sstable_size
    assert reconstructed_store._configuration.block_size == configuration_for_sample_manifest_0.block_size


def test_reconstruct_from_manifest_has_same_components(sample_manifest_1_with_events,
                                                       expected_state_of_manifest_1):
    # GIVEN
    manifest = sample_manifest_1_with_events

    # WHEN
    reconstructed_store = LsmStorage.reconstruct_from_manifest(manifest_path=manifest.file.path)

    # THEN
    expected_memtable = expected_state_of_manifest_1[0]
    expected_immutable_memtables = expected_state_of_manifest_1[1]
    expected_sstables_level0 = expected_state_of_manifest_1[2]
    expected_sstables_levels = expected_state_of_manifest_1[3]
    assert reconstructed_store.state.memtable == expected_memtable
    assert reconstructed_store.state.immutable_memtables == expected_immutable_memtables
    assert reconstructed_store.state.sstables_level0 == expected_sstables_level0
    assert reconstructed_store.state.sstables_levels == expected_sstables_levels


def test_compact_l0_writes_to_manifest(store_with_multiple_l0_sstables):
    # GIVEN
    store = store_with_multiple_l0_sstables
    manifest = store.manifest

    # WHEN/THEN
    with mock.patch.object(manifest, 'add_event') as mocked_add_event_to_manifest:
        # WHEN
        store.force_compaction_l0()

        # THEN
        mocked_add_event_to_manifest.assert_called_once()
        called_with_event = mocked_add_event_to_manifest.call_args[1]['event']
        assert isinstance(called_with_event, CompactionEvent)


def test_compact_l1_writes_to_manifest(store_with_four_l1_and_one_l2_sstables):
    # GIVEN
    store = store_with_four_l1_and_one_l2_sstables
    manifest = store.manifest

    # WHEN/THEN
    with mock.patch.object(manifest, 'add_event') as mocked_add_event_to_manifest:
        # WHEN
        store.force_compaction_l1_or_more_level(level=1)

        # THEN
        mocked_add_event_to_manifest.assert_called_once()
        called_with_event = mocked_add_event_to_manifest.call_args[1]['event']
        assert isinstance(called_with_event, CompactionEvent)


def test_flush_writes_to_manifest(store_with_multiple_immutable_memtables):
    # GIVEN
    store = store_with_multiple_immutable_memtables
    store._configuration.max_l0_sstables = 10
    store._configuration.levels_ratio = 10
    manifest = store.manifest

    # WHEN/THEN
    with mock.patch.object(manifest, 'add_event') as mocked_add_event_to_manifest:
        # WHEN
        store.flush_next_immutable_memtable()

        # THEN
        mocked_add_event_to_manifest.assert_called()
        called_with_event = mocked_add_event_to_manifest.call_args[1]['event']
        assert isinstance(called_with_event, FlushEvent)


def test_close_flushes_everything(store_with_multiple_immutable_memtables_and_one_memtable):
    # GIVEN
    store = store_with_multiple_immutable_memtables_and_one_memtable
    assert len(store.state.immutable_memtables) > 0
    assert store.state.memtable.approximate_size > 0
    assert len(store.state.sstables_level0) == 0

    # WHEN
    store.close()

    # THEN
    assert store.state.memtable.approximate_size == 0
    assert len(store.state.immutable_memtables) == 0
    assert len(store.state.sstables_level0) == 3
