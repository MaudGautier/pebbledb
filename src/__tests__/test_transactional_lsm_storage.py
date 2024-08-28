import threading
from unittest import mock

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
