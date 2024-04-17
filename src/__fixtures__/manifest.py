import os

import pytest

from src.__fixtures__.constants import TEST_SSTABLE_FIXTURES_DIRECTORY, TEST_DIRECTORY
from src.manifest import ManifestFile, FlushEvent, CompactionEvent, Configuration


@pytest.fixture
def empty_manifest_configuration():
    return Configuration(
        nb_levels=10,
        levels_ratio=0.1,
        max_l0_sstables=10,
        max_sstable_size=1000,
        block_size=100,
    )


@pytest.fixture
def empty_manifest(empty_manifest_configuration):
    path = f"{TEST_SSTABLE_FIXTURES_DIRECTORY}/empty_manifest.txt"

    yield ManifestFile.create(path=path, configuration=empty_manifest_configuration)

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)


@pytest.fixture
def manifest_path_with_no_file():
    return f"{TEST_DIRECTORY}/manifest_that_does_not_exist.txt"


@pytest.fixture
def events_for_sample_manifest_file_1(sstable_one_block_1, sstable_one_block_2,
                                      sstable_one_block_3, sstable_one_block_4):
    event1 = FlushEvent(sstable=sstable_one_block_1)
    event2 = FlushEvent(sstable=sstable_one_block_2)
    event3 = CompactionEvent(
        input_sstables=[sstable_one_block_1, sstable_one_block_2],
        output_sstables=[sstable_one_block_3],
        level=0
    )
    event4 = FlushEvent(sstable=sstable_one_block_4)

    return [event1, event2, event3, event4]


@pytest.fixture
def configuration_for_sample_manifest_file_1():
    return Configuration(
        nb_levels=6,
        levels_ratio=0.1,
        max_l0_sstables=10,
        max_sstable_size=1000,
        block_size=100,
    )


@pytest.fixture
def sample_manifest_file_1(events_for_sample_manifest_file_1, configuration_for_sample_manifest_file_1):
    # Configure manifest
    configuration = configuration_for_sample_manifest_file_1
    path = f"{TEST_SSTABLE_FIXTURES_DIRECTORY}/sample_manifest_1.txt"
    manifest_file = ManifestFile.create(path=path, configuration=configuration)

    # Add events
    for event in events_for_sample_manifest_file_1:
        manifest_file.write_event(event=event)

    # Yield
    yield manifest_file

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)
