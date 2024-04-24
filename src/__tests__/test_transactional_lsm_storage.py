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
