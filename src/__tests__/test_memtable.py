from unittest import mock

from src.memtable import MemTable
from src.record import Record
from src.sequence_number_generator import SequenceNumberGenerator


def test_can_put_and_retrieve(empty_memtable):
    # GIVEN
    memtable = empty_memtable

    # WHEN
    memtable.put(key=b'key', value=b"value")
    retrieved_value = memtable.get(key=b'key')

    # THEN
    assert retrieved_value == b'value'


def test_returns_none_if_not_found(empty_memtable):
    # GIVEN
    memtable = empty_memtable

    # WHEN
    memtable.put(key=b'key', value=b"value")
    retrieved_value = memtable.get(key=b'key1')

    # THEN
    assert retrieved_value is None


def test_scan(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    for key in [b'1', b'4', b'6', b'9']:
        memtable.put(key=key, value=key)

    # WHEN
    scanned_records = list(memtable.scan(lower=b'0', upper=b'5'))

    # THEN
    expected_records = [
        Record(key=b'1', value=b'1'),
        Record(key=b'4', value=b'4')
    ]
    assert scanned_records == expected_records


def test_writes_to_wal_when_inserting(empty_memtable):
    # GIVEN
    memtable = empty_memtable

    # WHEN/THEN
    with mock.patch.object(memtable.wal, 'insert') as mocked_insert_in_wal:
        # WHEN
        memtable.put(key=b'key', value=b'value')

        # THEN
        mocked_insert_in_wal.assert_called_once()


def test_can_recover(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    for key in [b'1', b'4', b'6', b'9']:
        memtable.put(key=key, value=key)
    wal_path = memtable.wal.path

    # WHEN
    resulting_memtable = memtable.create_from_wal(wal_path=wal_path)

    # THEN
    assert [record for record in resulting_memtable.map] == [record for record in memtable.map]


def test_equal(empty_memtable, empty_memtable2):
    # GIVEN
    memtable1 = empty_memtable
    all_keys = [27, 0, 2, 30, 45, 3, 12, 25, 4, 5, 8, 50]
    for key in all_keys:
        memtable1.put(key=str(key).encode("utf-8"), value=str(key).encode(encoding="utf-8"))

    SequenceNumberGenerator.reset()  # Memtables can be equal only if the same sequence numbers
    memtable2 = empty_memtable2
    for key in all_keys:
        memtable2.put(key=str(key).encode("utf-8"), value=str(key).encode(encoding="utf-8"))

    # WHEN/THEN
    assert memtable1 == memtable2


def test_not_equal_if_different_nodes(empty_memtable, empty_memtable2):
    # GIVEN
    memtable1 = empty_memtable
    memtable2 = empty_memtable2
    keys1 = [3, 5]
    keys2 = [3, 6]
    for key in keys1:
        memtable1.put(key=str(key).encode("utf-8"), value=str(key).encode(encoding="utf-8"))
    for key in keys2:
        memtable2.put(key=str(key).encode("utf-8"), value=str(key).encode(encoding="utf-8"))

    # WHEN/THEN
    assert memtable1 != memtable2


def test_can_recover_with_corrupted_wal(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    key_value_pairs = [(b'1', b'value1'), (b'4', b'value4'), (b'6', b'value6'), (b'9', b'value9')]
    for key, value in key_value_pairs:
        memtable.put(key=key, value=value)
    wal_path = memtable.wal.path

    # Corrupt the last one
    with open(memtable.wal.path, "rb") as file:
        data = file.read()
    with open(memtable.wal.path, "wb") as file:
        file.write(data[:-5])

    # WHEN
    resulting_memtable = memtable.create_from_wal(wal_path=wal_path)

    # THEN
    resulting_records = [record for record in resulting_memtable.map]
    all_records = [record for record in memtable.map]
    all_records_but_corrupted_one = all_records[:-1]
    assert resulting_records == all_records_but_corrupted_one


def test_scan_with_sequence_numbers_returns_only_the_most_recent_one(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    keys = [b'1', b'1', b'1']
    for key in keys:
        memtable.put(key=key, value=key)

    # WHEN
    scanned_records = list(memtable.scan())

    # THEN
    expected_record = Record(key=b'1', value=b'1')
    assert scanned_records == [expected_record]
    assert scanned_records[0].sequence_number == len(keys) - 1


def test_get_when_duplicates_returns_only_the_most_recent_one(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    key_value_pairs = [(b'1', b'1'), (b'1', b'2'), (b'1', b'3')]
    for key, value in key_value_pairs:
        memtable.put(key=key, value=value)

    # WHEN
    value = memtable.get(key=b'1')

    # THEN
    assert value == b'3'


def test_get_when_duplicates_and_snapshot_returns_only_the_most_recent_one_below_snapshot(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    key_value_pairs = [(b'1', b'1'), (b'1', b'2'), (b'1', b'3')]
    for key, value in key_value_pairs:
        memtable.put(key=key, value=value)

    # WHEN
    value = memtable.get(key=b'1', snapshot=1)

    # THEN
    assert value == b'2'
