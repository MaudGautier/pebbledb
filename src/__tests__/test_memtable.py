from src.memtable import MemTable


def test_can_put_and_retrieve():
    # GIVEN
    memtable = MemTable()

    # WHEN
    memtable.put(key="key", value=b"value")
    retrieved_value = memtable.get(key="key")

    # THEN
    assert retrieved_value == b'value'


def test_returns_None_if_not_found():
    # GIVEN
    memtable = MemTable()

    # WHEN
    memtable.put(key="key", value=b"value")
    retrieved_value = memtable.get(key="key1")

    # THEN
    assert retrieved_value is None
