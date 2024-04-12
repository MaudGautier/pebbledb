import os
import time

import pytest

from src.memtable import MemTable

TEST_SSTABLE_FIXTURES_DIRECTORY = "./test_sstable_fixtures"
TEST_DIRECTORY = "./test_store"

if not os.path.exists(TEST_SSTABLE_FIXTURES_DIRECTORY):
    os.makedirs(TEST_SSTABLE_FIXTURES_DIRECTORY)

if not os.path.exists(TEST_DIRECTORY):
    os.makedirs(TEST_DIRECTORY)


@pytest.fixture
def temporary_sstable_path():
    return f"{TEST_DIRECTORY}/{time.time() * 1_000_000}.sst"


@pytest.fixture
def temporary_sstable_path_2():
    return f"{TEST_DIRECTORY}/{time.time() * 1_000_000}.sst"


@pytest.fixture
def temporary_manifest_file_name():
    return f"{TEST_DIRECTORY}/manifest.sst"


def cleanup_files():
    for filename in os.listdir(TEST_DIRECTORY):
        os.remove(f"{TEST_DIRECTORY}/{filename}")


@pytest.fixture(autouse=True)
def clean_files(request):
    yield

    # Cleanup code
    request.addfinalizer(cleanup_files)
