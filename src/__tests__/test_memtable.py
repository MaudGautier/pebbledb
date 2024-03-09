from src.memtable import MemTable


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

    # WHEN/THEN
    print(memtable.map.read_data())
    expected_keys = ["1", "4"]
    i = 0
    for item in memtable.scan(lower="0", upper="5"):
        assert item == expected_keys[i].encode(encoding="utf-8")
        i += 1
