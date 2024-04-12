from contextlib import nullcontext as does_not_raise

import pytest

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
