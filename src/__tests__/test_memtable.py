from unittest import mock

from src.memtable import MemTable
from src.record import Record


def test_can_put_and_retrieve(empty_memtable):
    # GIVEN
    memtable = empty_memtable

    # WHEN
    memtable.put(key="key", value=b"value")
    retrieved_value = memtable.get(key="key")

    # THEN
    assert retrieved_value == b'value'


def test_returns_none_if_not_found(empty_memtable):
    # GIVEN
    memtable = empty_memtable

    # WHEN
    memtable.put(key="key", value=b"value")
    retrieved_value = memtable.get(key="key1")

    # THEN
    assert retrieved_value is None


def test_scan(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    for key in ["1", "4", "6", "9"]:
        memtable.put(key=key, value=key.encode(encoding="utf-8"))

    # WHEN
    scanned_records = list(memtable.scan(lower="0", upper="5"))

    # THEN
    expected_records = [
        Record(key="1", value="1".encode(encoding="utf-8")),
        Record(key="4", value="4".encode(encoding="utf-8"))
    ]
    assert scanned_records == expected_records


def test_writes_to_wal_when_inserting(empty_memtable):
    # GIVEN
    memtable = empty_memtable

    # WHEN/THEN
    with mock.patch.object(memtable.wal, 'insert') as mocked_insert_in_wal:
        # WHEN
        memtable.put(key="key", value=b'value')

        # THEN
        mocked_insert_in_wal.assert_called_once()


def test_can_recover(empty_memtable):
    # GIVEN
    memtable = empty_memtable
    for key in ["1", "4", "6", "9"]:
        memtable.put(key=key, value=key.encode(encoding="utf-8"))
    wal_path = memtable.wal.path

    # WHEN
    resulting_memtable = memtable.create_from_wal(wal_path=wal_path)

    # THEN
    assert [record for record in resulting_memtable.map] == [record for record in memtable.map]


def test_equal(empty_memtable, empty_memtable2):
    # GIVEN
    memtable1 = empty_memtable
    memtable2 = empty_memtable2
    all_keys = [27, 0, 2, 30, 45, 3, 12, 25, 4, 5, 8, 50]
    for key in all_keys:
        memtable1.put(key=str(key), value=str(key).encode(encoding="utf-8"))
        memtable2.put(key=str(key), value=str(key).encode(encoding="utf-8"))

    # WHEN/THEN
    assert memtable1 == memtable2


def test_not_equal_if_different_nodes(empty_memtable, empty_memtable2):
    # GIVEN
    memtable1 = empty_memtable
    memtable2 = empty_memtable2
    keys1 = [3, 5]
    keys2 = [3, 6]
    for key in keys1:
        memtable1.put(key=str(key), value=str(key).encode(encoding="utf-8"))
    for key in keys2:
        memtable2.put(key=str(key), value=str(key).encode(encoding="utf-8"))

    # WHEN/THEN
    assert memtable1 != memtable2
