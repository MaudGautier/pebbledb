import os

import pytest

from src.__fixtures__.constants import TEST_SSTABLE_FIXTURES_DIRECTORY, TEST_DIRECTORY
from src.wal import WriteAheadLog


@pytest.fixture
def empty_wal():
    path = f"{TEST_SSTABLE_FIXTURES_DIRECTORY}/empty_wal.wal"
    yield WriteAheadLog.create(path=path)

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)


@pytest.fixture
def wal_path_with_no_file():
    return f"{TEST_DIRECTORY}/wal_that_does_not_exist.wal"
