import threading
from unittest import mock
from unittest.mock import ANY, call

from src.iterators import MergingIterator, ConcatenatingIterator
from src.lsm_storage import LsmStorage
from src.record import Record
from src.transactional_lsm_storage import TransactionalLsmStorage


def test_put_a_new_record_updates_last_committed_number(empty_transactional_store):
    # GIVEN
    transactional_store = empty_transactional_store
    assert transactional_store.last_committed_sequence_number == -1

    # WHEN
    transactional_store.put(key=b'0', value=b'0')
    transactional_store.put(key=b'1', value=b'1')
    transactional_store.put(key=b'2', value=b'2')

    # THEN
    assert transactional_store.last_committed_sequence_number == 2


def test_put_duplicates_in_transactional_store_writes_all_versions_when_flush(
        transactional_store_with_duplicates_in_immutable_memtable,
        key_values_for_transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable
    key_values = key_values_for_transactional_store_with_duplicates_in_immutable_memtable

    # WHEN
    store._trigger_flush()

    # THEN
    record_0 = Record(key=key_values[0][0], value=key_values[0][1])
    record_1 = Record(key=key_values[1][0], value=key_values[1][1])
    record_0.sequence_number = 0
    record_1.sequence_number = 1
    encoded_records = record_1.to_bytes() + record_0.to_bytes()  # Should be written most recent first
    assert store.state.sstables_level0[0].file.read().startswith(encoded_records)


def test_get_in_transactional_store_finds_correct_version_in_memtables(
        transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable

    # WHEN
    value_a = store.get(key=b'keyA')
    value_b = store.get(key=b'keyB')
    value_c = store.get(key=b'keyC')

    # THEN
    assert value_a == b'valueA3'
    assert value_b == b'valueB2'
    assert value_c == b'valueC1'


def test_get_in_transactional_store_finds_correct_version_in_ss_tables_level0(
        transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable
    store._flush()
    store._flush()
    assert len(store.state.immutable_memtables) == 1
    assert len(store.state.sstables_level0) == 2

    # WHEN
    value_a = store.get(key=b'keyA')
    value_b = store.get(key=b'keyB')
    value_c = store.get(key=b'keyC')

    # THEN
    assert value_a == b'valueA3'
    assert value_b == b'valueB2'
    assert value_c == b'valueC1'


def test_get_uses_previous_committed_version(transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable
    original_value = store.get(key=b'keyA')

    # WHEN
    results = []

    # This start_event, together with the `start_event.wait()` makes sure that both threads will start at the same time
    # (it is important to remove flakiness)
    start_event = threading.Event()

    def get_with_return():
        start_event.wait()  # Wait for the signal to start
        value = store.get(key=b'keyA')
        results.append(value)

    def put_wrapper():
        start_event.wait()  # Wait for the signal to start
        store.put(key=b'keyA', value=b'new_value')

    put_thread = threading.Thread(target=put_wrapper)
    get_thread = threading.Thread(target=get_with_return)

    # Start threads
    put_thread.start()
    get_thread.start()

    # Signal threads to start operations
    start_event.set()

    # Join threads
    put_thread.join()
    get_thread.join()

    # THEN
    assert results == [original_value]


def test_scan_in_transactional_store_finds_correct_version_in_memtables(
        transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable

    # WHEN
    values = [record.value for record in store.scan(lower=b'keyA', upper=b'keyB')]

    # THEN
    assert values == [b'valueA3', b'valueB2']


def test_scan_in_transactional_store_finds_correct_version_in_ss_tables_level0(
        transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable
    store._flush()
    store._flush()
    assert len(store.state.immutable_memtables) == 1
    assert len(store.state.sstables_level0) == 2

    # WHEN
    values = [record.value for record in store.scan(lower=b'keyA', upper=b'keyB')]

    # THEN
    assert values == [b'valueA3', b'valueB2']


def test_scan_uses_previous_committed_version(transactional_store_with_duplicates_in_immutable_memtable):
    # GIVEN
    store = transactional_store_with_duplicates_in_immutable_memtable
    original_values = [record.value for record in store.scan(lower=b'keyA', upper=b'keyB')]

    # WHEN
    results = []

    # This start_event, together with the `start_event.wait()` makes sure that both threads will start at the same time
    # (it is important to remove flakiness)
    start_event = threading.Event()

    def get_with_return():
        start_event.wait()  # Wait for the signal to start
        values = [record.value for record in store.scan(lower=b'keyA', upper=b'keyB')]
        results.append(values)

    def put_wrapper():
        start_event.wait()  # Wait for the signal to start
        store.put(key=b'keyA', value=b'new_value')

    put_thread = threading.Thread(target=put_wrapper)
    get_thread = threading.Thread(target=get_with_return)

    # Start threads
    put_thread.start()
    get_thread.start()

    # Signal threads to start operations
    start_event.set()

    # Join threads
    put_thread.join()
    get_thread.join()

    # THEN
    assert results == [original_values]


def test_create_initializes_with_correct_sequence_number():
    # GIVEN
    transactional_directory = "./test_transactional_directory"

    # WHEN/THEN
    with mock.patch.object(LsmStorage, 'create') as mocked_lsm_create:
        # WHEN
        transactional_store = TransactionalLsmStorage.create(directory=transactional_directory)

        # THEN
        mocked_lsm_create.assert_called_once()

    # THEN
    assert transactional_store.last_committed_sequence_number == -1


def test_reconstruct_from_manifest_selects_the_correct_sequence_number(sample_manifest_1_with_events,
                                                                       records_for_sstable_one_block):
    # GIVEN
    manifest = sample_manifest_1_with_events

    # WHEN
    reconstructed_store = TransactionalLsmStorage.reconstruct(manifest_path=manifest.file.path)

    # THEN
    expected_last_sequence_number = len(records_for_sstable_one_block) - 1  # Because manifest made of these events
    assert reconstructed_store.last_committed_sequence_number == expected_last_sequence_number


def test_get_adds_snapshot_to_list_and_then_removes_it(empty_transactional_store):
    # GIVEN
    store = empty_transactional_store
    initial_size = len(store.current_snapshots)

    # Define a wrapper for the _get method that includes the assertion
    def wrapper(*args, **kwargs):
        # THEN: Before calling `_get`, the snapshot must have been added to the list of current snapshots
        assert len(store.current_snapshots) == initial_size + 1

        store._get(*args, **kwargs)

    # Patch the _get method with our wrapper
    with mock.patch.object(store, '_get', wrapper=wrapper):
        # WHEN
        store.get(key=b'key')

    # THEN: The snapshot must be removed from the list after `_get` has completed
    assert len(store.current_snapshots) == initial_size


def test_scan_adds_snapshot_to_list_and_then_removes_it(empty_transactional_store):
    # GIVEN
    store = empty_transactional_store
    initial_size = len(store.current_snapshots)

    # Define a wrapper for the _get method that includes the assertion
    def wrapper(*args, **kwargs):
        # THEN: Before calling `_scan`, the snapshot must have been added to the list of current snapshots
        assert len(store.current_snapshots) == initial_size + 1

        store._scan(*args, **kwargs)

    # Patch the _scan method with our wrapper
    with mock.patch.object(store, '_scan', wrapper=wrapper):
        # WHEN
        store.scan(lower=b'lower', upper=b'upper')

    # THEN: The snapshot must be removed from the list after `_get` has completed
    assert len(store.current_snapshots) == initial_size


# TODO: SELECTED!!!
def test_compact_l0_on_non_transactional_store_calls_merging_iterator_with_filter_duplicates_true(
        store_with_multiple_l0_sstables):
    # GIVEN
    store = store_with_multiple_l0_sstables
    nb_l0_sstables = len(store.state.sstables_level0)

    # WHEN/THEN
    with mock.patch('src.lsm_storage.MergingIterator') as mock_merging_iterator, \
            mock.patch('src.lsm_storage.CompactSSTableIterator') as mock_compact_sstable_iterator:
        # WHEN
        store._compact_l0()

        # THEN
        # Check that compact sstable was called the right number of times
        assert mock_compact_sstable_iterator.call_count == nb_l0_sstables
        # Check that MergingIterator was called with the expected arguments
        mock_merging_iterator.assert_called_once_with(
            iterators=[ANY for _ in range(nb_l0_sstables)],
            # filter_duplicates=True # By default
        )
        # NB: iterators should be the returns of mock_compact_sstable_iterator (but assuming this test is good enough)


def test_compact_l0_on_transactional_store_calls_merging_iterator_with_filter_duplicates_false(
        transactional_store_with_duplicates_in_l0_sstables):
    # GIVEN
    store = transactional_store_with_duplicates_in_l0_sstables

    # Define a wrapper for the init method and record calls
    init_wrapper_calls = []  # Storage for calls
    original_init = MergingIterator.__init__  # Save the original constructor

    def init_wrapper(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        init_wrapper_calls.append(call(*args, **kwargs))

    # WHEN/THEN
    with mock.patch.object(MergingIterator, '__init__', new=init_wrapper):
        # WHEN
        store._compact_l0()

        # THEN
        # Check calls were as expected
        assert call(iterators=ANY, filter_duplicates=False) in init_wrapper_calls
        assert len(init_wrapper_calls) == 1  # Ensure it was called once


def test_compact_l1_on_non_transactional_store_calls_concatenating_iterator_without_filter_duplicates(
        store_with_multiple_l1_sstables):
    # GIVEN
    store = store_with_multiple_l1_sstables
    nb_l1_sstables = len(store.state.sstables_levels[0])

    # WHEN/THEN
    with mock.patch('src.lsm_storage.ConcatenatingIterator') as mock_concatenating_iterator, \
            mock.patch('src.lsm_storage.CompactSSTableIterator') as mock_compact_sstable_iterator:
        # WHEN
        store._compact_l1_or_more(level=1)

        # THEN
        # Check that compact sstable was called the right number of times
        assert mock_compact_sstable_iterator.call_count == nb_l1_sstables
        # Check that MergingIterator was called with the expected arguments
        mock_concatenating_iterator.assert_called_once_with(
            iterators=[ANY for _ in range(nb_l1_sstables)]
            # filter_duplicates not passed
        )
        # NB: iterators should be the returns of mock_compact_sstable_iterator (but assuming this test is good enough)


def test_compact_l1_on_transactional_store_calls_concatenating_iterator_without_filter_duplicates(
        transactional_store_with_duplicates_in_l1_sstables):
    # GIVEN
    store = transactional_store_with_duplicates_in_l1_sstables

    # Define a wrapper for the init method and record calls
    init_wrapper_calls = []  # Storage for calls
    original_init = ConcatenatingIterator.__init__  # Save the original constructor

    def init_wrapper(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        init_wrapper_calls.append(call(*args, **kwargs))

    # WHEN/THEN
    with mock.patch.object(ConcatenatingIterator, '__init__', new=init_wrapper):
        # WHEN
        store._compact_l1_or_more(level=1)

        # THEN
        # Check calls were as expected
        assert call(iterators=ANY) in init_wrapper_calls  # No filter duplicates
        assert len(init_wrapper_calls) == 1  # Ensure it was called once


def test_compact_should_keep_only_valid_records(empty_transactional_store):
    # GIVEN
    key_value_pairs = [(b'keyA', b'valueA1'),  # 0
                       (b'keyA', b'valueA2'),  # 1
                       (b'keyA', b'valueA3'),  # 2
                       (b'keyB', b'valueB1'),  # 3
                       (b'keyB', b'valueB2'),  # 4
                       (b'keyC', b'valueC1'),  # 5
                       (b'keyC', b'valueC2'),  # 6
                       (b'keyC', b'valueC3'),  # 7
                       (b'keyD', b'valueD1')]  # 8
    store = empty_transactional_store
    store._configuration.max_sstable_size = 10000
    store._configuration.block_size = 30
    for key, value in key_value_pairs:
        store.put(key=key, value=value)
    store._freeze()
    store._flush()
    assert len(store.state.sstables_level0) == 1
    assert len(store.state.sstables_levels[0]) == 0

    # WHEN
    store.last_committed_sequence_number = 6
    store._compact_l0()

    # THEN
    assert len(store.state.sstables_level0) == 0
    assert len(store.state.sstables_levels[0]) == 1

    # Read all data blocks
    encoded_data_blocks = b''
    for i in range(len(store.state.sstables_levels[0][0].meta_blocks)):
        encoded_data_blocks += store.state.sstables_levels[0][0].read_data_block(block_id=i).data

    assert b'valueA1' not in encoded_data_blocks  # sequence_number: 0
    assert b'valueA2' not in encoded_data_blocks  # sequence_number: 1
    assert b'valueA3' in encoded_data_blocks  # sequence_number: 2 --- Last below snapshot => kept
    assert b'valueB1' not in encoded_data_blocks  # sequence_number: 3
    assert b'valueB2' in encoded_data_blocks  # sequence_number: 4 --- Last below snapshot => kept
    assert b'valueC1' not in encoded_data_blocks  # sequence_number: 5
    assert b'valueC2' in encoded_data_blocks  # sequence_number: 6 --- Last below snapshot => kept
    assert b'valueC3' in encoded_data_blocks  # sequence_number: 7 --- Above snapshot => kept
    assert b'valueD1' in encoded_data_blocks  # sequence_number: 8 --- Above snapshot => kept
