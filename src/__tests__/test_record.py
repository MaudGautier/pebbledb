from src.record import Record


def test_can_decode_record():
    # GIVEN
    in_record = Record(key="key", value=b"value")
    assert in_record.key_size == 3
    assert in_record.value_size == 5
    in_bytes = in_record.to_bytes()

    # WHEN
    out_record = Record.from_bytes(in_bytes)

    # THEN
    assert out_record == in_record
