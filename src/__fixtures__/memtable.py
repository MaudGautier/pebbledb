import pytest

from src.__fixtures__.constants import TEST_DIRECTORY
from src.memtable import MemTable


@pytest.fixture
def empty_memtable():
    return MemTable.create(directory=TEST_DIRECTORY)

@pytest.fixture
def empty_memtable2():
    return MemTable.create(directory=TEST_DIRECTORY)
