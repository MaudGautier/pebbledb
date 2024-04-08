from src.manifest import Manifest, FlushEvent, CompactionEvent
from src.__fixtures__.sstable import sstable_four_blocks, records_for_sstable_four_blocks, sstable_one_block, \
    records_for_sstable_one_block, sstable_one_block_2, records_for_sstable_one_block_2


def test_reconstruct_from_empty_events():
    # GIVEN
    events = []
    manifest = Manifest(events=events)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0
    for level in store.ss_tables_levels:
        assert len(level) == 0


def test_reconstruct_from_one_flush_event(sstable_four_blocks):
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


def test_reconstruct_from_two_flush_events(sstable_four_blocks, sstable_one_block):
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


def test_reconstruct_from_one_flush_event_and_one_compaction_event(sstable_four_blocks, sstable_one_block):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block
    events = [FlushEvent(sstable=sstable1),
              CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], output_level=1)]
    manifest = Manifest(events=events)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0
    assert len(store.ss_tables_levels[0]) == 1
    assert store.ss_tables_levels[0][0] == sstable2


def test_reconstruct_from_one_flush_event_and_two_compaction_events(sstable_four_blocks,
                                                                    sstable_one_block,
                                                                    sstable_one_block_2):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block
    sstable3 = sstable_one_block_2
    events = [FlushEvent(sstable=sstable1),
              CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], output_level=1),
              CompactionEvent(input_sstables=[sstable2], output_sstables=[sstable3], output_level=2)]
    manifest = Manifest(events=events)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0  # l0
    assert len(store.ss_tables_levels[0]) == 0  # l1
    assert len(store.ss_tables_levels[1]) == 1  # l2
    assert store.ss_tables_levels[1][0] == sstable3
