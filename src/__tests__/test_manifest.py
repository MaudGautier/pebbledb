from src.manifest import (
    Manifest,
    ManifestSSTable,
    FlushEvent,
    CompactionEvent,
    ManifestFlushRecord,
    ManifestCompactionRecord,
    ManifestSSTablesBlock,
)


def test_reconstruct_from_empty_events():
    # GIVEN
    events = []
    manifest = Manifest(events=events, nb_levels=3)

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
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 1
    for level in store.ss_tables_levels:
        assert len(level) == 0


def test_reconstruct_from_two_flush_events(sstable_four_blocks, sstable_one_block_1):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block_1
    events = [
        FlushEvent(sstable=sstable1),
        FlushEvent(sstable=sstable2)
    ]
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 2
    assert store.ss_tables[0] == sstable2
    assert store.ss_tables[1] == sstable1
    for level in store.ss_tables_levels:
        assert len(level) == 0


def test_reconstruct_from_one_flush_event_and_one_compaction_event(sstable_four_blocks, sstable_one_block_1):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block_1
    events = [
        FlushEvent(sstable=sstable1),
        CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0)
    ]
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0
    assert len(store.ss_tables_levels[0]) == 1
    assert store.ss_tables_levels[0][0] == sstable2


def test_reconstruct_from_one_flush_event_and_two_compaction_events(sstable_four_blocks,
                                                                    sstable_one_block_1,
                                                                    sstable_one_block_2):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block_1
    sstable3 = sstable_one_block_2
    events = [
        FlushEvent(sstable=sstable1),
        CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0),
        CompactionEvent(input_sstables=[sstable2], output_sstables=[sstable3], level=1)
    ]
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0  # l0
    assert len(store.ss_tables_levels[0]) == 0  # l1
    assert len(store.ss_tables_levels[1]) == 1  # l2
    assert store.ss_tables_levels[1][0] == sstable3


def test_reconstruct_from_one_flush_event_and_three_compaction_events(sstable_one_block_1,
                                                                      sstable_one_block_2,
                                                                      sstable_one_block_3,
                                                                      sstable_one_block_4):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstable3 = sstable_one_block_3
    sstable4 = sstable_one_block_4
    events = [
        FlushEvent(sstable=sstable1),
        CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0),
        CompactionEvent(input_sstables=[sstable2], output_sstables=[sstable3], level=1),
        CompactionEvent(input_sstables=[sstable3], output_sstables=[sstable4], level=2),
    ]
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0  # l0
    assert len(store.ss_tables_levels[0]) == 0  # l1
    assert len(store.ss_tables_levels[1]) == 0  # l2
    assert len(store.ss_tables_levels[2]) == 1  # l3
    assert store.ss_tables_levels[2][0] == sstable4


def test_reconstruct_from_two_flush_events_and_one_compaction_event(sstable_one_block_1,
                                                                    sstable_one_block_2,
                                                                    sstable_one_block_3):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstable3 = sstable_one_block_3
    events = [
        FlushEvent(sstable=sstable1),
        FlushEvent(sstable=sstable2),
        CompactionEvent(input_sstables=[sstable1, sstable2], output_sstables=[sstable3], level=0),
    ]
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0  # l0
    assert len(store.ss_tables_levels[0]) == 1  # l1
    assert store.ss_tables_levels[0][0] == sstable3


def test_reconstruct_from_two_flush_events_interspaced_with_compaction_events(sstable_one_block_1,
                                                                              sstable_one_block_2,
                                                                              sstable_one_block_3,
                                                                              sstable_one_block_4):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstable3 = sstable_one_block_3
    sstable4 = sstable_one_block_4
    events = [
        FlushEvent(sstable=sstable1),
        CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable3], level=0),
        FlushEvent(sstable=sstable2),
        CompactionEvent(input_sstables=[sstable2], output_sstables=[sstable4], level=0),
    ]
    manifest = Manifest(events=events, nb_levels=3)

    # WHEN
    store = manifest.reconstruct()

    # THEN
    assert len(store.ss_tables) == 0  # l0
    assert len(store.ss_tables_levels[0]) == 2  # l1
    assert store.ss_tables_levels[0][1] == sstable3
    assert store.ss_tables_levels[0][0] == sstable4


def test_encode_decode_manifest_sstable(sstable_one_block_1):
    # GIVEN
    sstable = sstable_one_block_1
    manifest_sstable = ManifestSSTable(sstable=sstable)

    # WHEN
    encoded_manifest_sstable = manifest_sstable.to_bytes()
    decoded_manifest_sstable = ManifestSSTable.from_bytes(data=encoded_manifest_sstable)

    # THEN
    assert manifest_sstable.sstable == decoded_manifest_sstable.sstable


def test_encode_decode_manifest_flush_record(sstable_one_block_1):
    # GIVEN
    sstable = sstable_one_block_1
    flush_event = FlushEvent(sstable=sstable)
    manifest_flush_record = ManifestFlushRecord(event=flush_event)

    # WHEN
    encoded_manifest_record = manifest_flush_record.to_bytes()
    decoded_manifest_record = ManifestFlushRecord.from_bytes(data=encoded_manifest_record)

    # THEN
    assert manifest_flush_record.event.sstable == decoded_manifest_record.event.sstable


def test_encode_decode_manifest_sstable_block(sstable_one_block_1, sstable_one_block_2):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstables_to_encode = [sstable1, sstable2]

    # WHEN
    encoded_block = ManifestSSTablesBlock(sstables=sstables_to_encode).to_bytes()
    decoded_sstables = ManifestSSTablesBlock.from_bytes(data=encoded_block).sstables

    # THEN
    assert decoded_sstables == sstables_to_encode


def test_encode_decode_manifest_compaction_record(sstable_one_block_1, sstable_one_block_2):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    compaction_event = CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0)

    manifest_compaction_record = ManifestCompactionRecord(event=compaction_event)

    # WHEN
    encoded_manifest_record = manifest_compaction_record.to_bytes()
    decoded_manifest_record = ManifestCompactionRecord.from_bytes(data=encoded_manifest_record)

    # THEN
    assert manifest_compaction_record.event.input_sstables == decoded_manifest_record.event.input_sstables
    assert manifest_compaction_record.event.output_sstables == decoded_manifest_record.event.output_sstables
    assert manifest_compaction_record.event.level == decoded_manifest_record.event.level
