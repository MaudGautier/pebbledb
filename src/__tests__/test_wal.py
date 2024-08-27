from contextlib import nullcontext as does_not_raise

import pytest

from src.record import Record
from src.wal import WriteAheadLog


def test_create_wal_from_existing_path_should_raise_an_error(empty_wal):
    # GIVEN
    path_with_file = empty_wal.path

    # WHEN/THEN
    with pytest.raises(ValueError):
        WriteAheadLog.create(path=path_with_file)


def test_open_wal_from_existing_path_should_not_raise_an_error(empty_wal):
    # GIVEN
    path_with_file = empty_wal.path

    # WHEN/THEN
    with does_not_raise():
        WriteAheadLog.open(path=path_with_file)


def test_create_wal_from_new_path_should_not_raise_an_error(wal_path_with_no_file):
    # GIVEN/WHEN/THEN
    with does_not_raise():
        WriteAheadLog.create(path=wal_path_with_no_file)


def test_open_wal_from_new_path_should_raise_an_error(wal_path_with_no_file):
    # GIVEN/WHEN/THEN
    with pytest.raises(ValueError):
        WriteAheadLog.open(path=wal_path_with_no_file)


def test_corrupted_wal_ignores_last_record(empty_wal):
    # GIVEN
    wal = empty_wal
    records = [
        Record(key=b'1', value=b'value1'),
        Record(key=b'3', value=b'value3'),
        Record(key=b'7', value=b'value7'),
        Record(key=b'9', value=b'value9'),
    ]
    for record in records:
        wal.insert(record=record)

    # Corrupt the last one
    with open(wal.path, "rb") as file:
        data = file.read()
    with open(wal.path, "wb") as file:
        file.write(data[:-5])

    # WHEN
    new_wal = WriteAheadLog.open(path=wal.path)
    read_records = new_wal.read_records()

    # THEN
    assert read_records == records[:-1]
