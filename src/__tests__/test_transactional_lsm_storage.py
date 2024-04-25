from src.record import Record


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
