from src.record import Record


def test_can_decode_record():
    # GIVEN
    in_record = Record(key=b'key', value=b"value")
    assert in_record.key_size == 3
    assert in_record.value_size == 5
    in_bytes = in_record.to_bytes()

    # WHEN
    out_record = Record.from_bytes(in_bytes)

    # THEN
    assert out_record == in_record


def test_snapshot_key_orders_sequence_numbers_in_reverse_for_identical_keys():
    # GIVEN
    record_a0 = Record(key=b'a', value='value')
    record_a1 = Record(key=b'a', value='value')
    record_a2 = Record(key=b'a', value='value')
    assert record_a0.sequence_number == 0
    assert record_a1.sequence_number == 1
    record_a2.sequence_number = 200

    # WHEN
    ordered_key_snapshots = sorted([
        record_a0.snapshot_key,
        record_a1.snapshot_key,
        record_a2.snapshot_key,
    ])

    # THEN
    expected_key_snapshots_order = [
        record_a2.snapshot_key,
        record_a1.snapshot_key,
        record_a0.snapshot_key,
    ]
    assert ordered_key_snapshots == expected_key_snapshots_order


def test_snapshot_key_provides_correct_ordering():
    # GIVEN
    record_b0 = Record(key=b'b', value='value')
    record_a0 = Record(key=b'a', value='value')
    record_a1 = Record(key=b'a', value='value')
    record_b1 = Record(key=b'b', value='value')
    assert record_b0.sequence_number == 0
    assert record_a0.sequence_number == 1
    assert record_a1.sequence_number == 2
    assert record_b1.sequence_number == 3

    # WHEN
    ordered_key_snapshots = sorted([
        record_b0.snapshot_key,
        record_a0.snapshot_key,
        record_a1.snapshot_key,
        record_b1.snapshot_key,
    ])

    # THEN
    expected_key_snapshots_order = [
        record_a1.snapshot_key,
        record_a0.snapshot_key,
        record_b1.snapshot_key,
        record_b0.snapshot_key,
    ]
    assert ordered_key_snapshots == expected_key_snapshots_order
