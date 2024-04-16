import os

import pytest

from src.__fixtures__.constants import TEST_SSTABLE_FIXTURES_DIRECTORY, TEST_DIRECTORY
from src.manifest import ManifestFile


@pytest.fixture
def empty_manifest_configuration():
    return {
        "nb_levels": 10,
        "levels_ratio": 0.1,
        "max_l0_sstables": 10,
        "max_sstable_size": 1000,
        "block_size": 100,
    }


@pytest.fixture
def empty_manifest(empty_manifest_configuration):
    path = f"{TEST_SSTABLE_FIXTURES_DIRECTORY}/empty_manifest.wal"

    yield ManifestFile.create(path=path, **empty_manifest_configuration)

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)


@pytest.fixture
def manifest_path_with_no_file():
    return f"{TEST_DIRECTORY}/manifest_that_does_not_exist.wal"
