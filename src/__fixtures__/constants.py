import os
import time

import pytest

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
