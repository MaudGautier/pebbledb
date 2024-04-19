import os
from collections import deque

import pytest

from src.__fixtures__.constants import TEST_SSTABLE_FIXTURES_DIRECTORY, TEST_DIRECTORY
from src.manifest import ManifestFile, FlushEvent, CompactionEvent, Configuration, Manifest
from src.memtable import MemTable


@pytest.fixture
def empty_manifest_file_configuration():
    return Configuration(
        nb_levels=10,
        levels_ratio=0.1,
        max_l0_sstables=10,
        max_sstable_size=1000,
        block_size=100,
    )


@pytest.fixture
def empty_manifest_file(empty_manifest_file_configuration):
    path = f"{TEST_DIRECTORY}/empty_manifest.txt"

    yield ManifestFile.create(path=path, configuration=empty_manifest_file_configuration)

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
    path = f"{TEST_DIRECTORY}/sample_manifest_1.txt"
    manifest_file = ManifestFile.create(path=path, configuration=configuration)

    # Add events
    for event in events_for_sample_manifest_file_1:
        manifest_file.write_event(event=event)

    # Yield
    yield manifest_file

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)


@pytest.fixture
def configuration_for_sample_manifest_0():
    return Configuration(
        nb_levels=6,
        levels_ratio=0.1,
        max_l0_sstables=10,
        max_sstable_size=1000,
        block_size=100,
    )


@pytest.fixture
def sample_manifest_0_without_events(configuration_for_sample_manifest_0):
    path = f"{TEST_DIRECTORY}/manifest_0.txt"
    configuration = configuration_for_sample_manifest_0
    manifest = Manifest.create(path=path, configuration=configuration)

    yield manifest

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)


@pytest.fixture
def path_to_manifest_1():
    return f"{TEST_DIRECTORY}/manifest_1.txt"


@pytest.fixture
def sample_manifest_1_with_events(events_for_sample_manifest_file_1, configuration_for_sample_manifest_file_1):
    path = f"{TEST_DIRECTORY}/manifest_1.txt"
    configuration = configuration_for_sample_manifest_file_1
    events = events_for_sample_manifest_file_1
    manifest = Manifest.create(path=path, configuration=configuration)

    # Add events
    for event in events:
        manifest.add_event(event=event)

    yield manifest

    # Cleanup code (Delete the file created by the fixture)
    os.remove(path)


@pytest.fixture
def expected_state_of_manifest_1(sstable_one_block_3, sstable_one_block_4,
                                 path_to_manifest_1, configuration_for_sample_manifest_file_1):
    directory = os.path.dirname(path_to_manifest_1)
    memtables = MemTable.create(directory=directory)
    immutable_memtables = deque()
    sstables_level0 = deque([sstable_one_block_4])
    sstables_levels = [deque() for _ in range(configuration_for_sample_manifest_file_1.nb_levels)]
    sstables_levels[0] = deque([sstable_one_block_3])

    return memtables, immutable_memtables, sstables_level0, sstables_levels
