from src.memtable import MemTable
from src.record import Record


def test_can_put_and_retrieve():
    # GIVEN
    memtable = MemTable()

    # WHEN
    memtable.put(key="key", value=b"value")
    retrieved_value = memtable.get(key="key")

    # THEN
    assert retrieved_value == b'value'


def test_returns_none_if_not_found():
    # GIVEN
    memtable = MemTable()

    # WHEN
    memtable.put(key="key", value=b"value")
    retrieved_value = memtable.get(key="key1")

    # THEN
    assert retrieved_value is None


def test_scan():
    # GIVEN
    memtable = MemTable()
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


def test_iterate():
    # GIVEN
    memtable = MemTable()
    keys = ["1", "6", "9", "4"]
    for key in keys:
        memtable.put(key=key, value=key.encode(encoding="utf-8"))

    # WHEN
    records = [record for record in memtable]

    # THEN
    expected_records = [Record(key=key, value=key.encode(encoding="utf-8")) for key in sorted(keys)]
    assert records == expected_records