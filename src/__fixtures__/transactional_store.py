import pytest

from src.__fixtures__.constants import TEST_DIRECTORY
from src.transactional_lsm_storage import TransactionalLsmStorage


@pytest.fixture
def empty_transactional_store():
    return TransactionalLsmStorage.create(directory=TEST_DIRECTORY)
