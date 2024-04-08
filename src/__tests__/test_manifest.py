from src.manifest import Manifest, FlushEvent
from src.__fixtures__.sstable import sstable_four_blocks, records_for_sstable_four_blocks, sstable_one_block, \
    records_for_sstable_one_block


def test_reconstructing_a_state_from_empty_events_creates_an_empty_LsmStorage():
    # GIVEN
    events = []
    manifest = Manifest(events=events)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0
    for level in store.ss_tables_levels:
        assert len(level) == 0


def test_reconstructing_a_state_from_one_flush_event_creates_an_LsmStore_with_one_sstable(sstable_four_blocks):
    # GIVEN
    sstable = sstable_four_blocks
    events = [FlushEvent(sstable=sstable)]
    manifest = Manifest(events=events)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 1
    for level in store.ss_tables_levels:
        assert len(level) == 0


def test_reconstructing_a_state_from_two_flush_events_creates_an_LsmStore_with_two_sstables(sstable_four_blocks,
                                                                                            sstable_one_block):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block
    events = [FlushEvent(sstable=sstable1), FlushEvent(sstable=sstable2)]
    manifest = Manifest(events=events)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 2
    assert store.ss_tables[0] == sstable2
    assert store.ss_tables[1] == sstable1
    for level in store.ss_tables_levels:
        assert len(level) == 0
