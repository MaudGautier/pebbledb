from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from src.manifest import (
    Manifest,
    ManifestSSTable,
    FlushEvent,
    CompactionEvent,
    ManifestFlushRecord,
    ManifestCompactionRecord,
    ManifestSSTablesBlock,
    ManifestRecord,
    ManifestHeader,
    ManifestFile,
)


def test_reconstruct_from_empty_events(empty_manifest):
    # GIVEN
    events = []
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 0
    for level in ss_tables_levels:
        assert len(level) == 0


def test_reconstruct_from_one_flush_event(sstable_four_blocks, empty_manifest):
    # GIVEN
    sstable = sstable_four_blocks
    events = [FlushEvent(sstable=sstable)]
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 1
    for level in ss_tables_levels:
        assert len(level) == 0


def test_reconstruct_from_two_flush_events(sstable_four_blocks, sstable_one_block_1, empty_manifest):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block_1
    events = [
        FlushEvent(sstable=sstable1),
        FlushEvent(sstable=sstable2)
    ]
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 2
    assert level0[0] == sstable2
    assert level0[1] == sstable1
    for level in ss_tables_levels:
        assert len(level) == 0


def test_reconstruct_from_one_flush_event_and_one_compaction_event(sstable_four_blocks, sstable_one_block_1,
                                                                   empty_manifest):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block_1
    events = [
        FlushEvent(sstable=sstable1),
        CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0)
    ]
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]


    # THEN
    assert len(level0) == 0
    assert len(ss_tables_levels[0]) == 1
    assert ss_tables_levels[0][0] == sstable2


def test_reconstruct_from_one_flush_event_and_two_compaction_events(sstable_four_blocks,
                                                                    sstable_one_block_1,
                                                                    sstable_one_block_2,
                                                                    empty_manifest):
    # GIVEN
    sstable1 = sstable_four_blocks
    sstable2 = sstable_one_block_1
    sstable3 = sstable_one_block_2
    events = [
        FlushEvent(sstable=sstable1),
        CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0),
        CompactionEvent(input_sstables=[sstable2], output_sstables=[sstable3], level=1)
    ]
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 0  # l0
    assert len(ss_tables_levels[0]) == 0  # l1
    assert len(ss_tables_levels[1]) == 1  # l2
    assert ss_tables_levels[1][0] == sstable3


def test_reconstruct_from_one_flush_event_and_three_compaction_events(sstable_one_block_1,
                                                                      sstable_one_block_2,
                                                                      sstable_one_block_3,
                                                                      sstable_one_block_4,
                                                                      empty_manifest):
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
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 0  # l0
    assert len(ss_tables_levels[0]) == 0  # l1
    assert len(ss_tables_levels[1]) == 0  # l2
    assert len(ss_tables_levels[2]) == 1  # l3
    assert ss_tables_levels[2][0] == sstable4


def test_reconstruct_from_two_flush_events_and_one_compaction_event(sstable_one_block_1,
                                                                    sstable_one_block_2,
                                                                    sstable_one_block_3,
                                                                    empty_manifest):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstable3 = sstable_one_block_3
    events = [
        FlushEvent(sstable=sstable1),
        FlushEvent(sstable=sstable2),
        CompactionEvent(input_sstables=[sstable1, sstable2], output_sstables=[sstable3], level=0),
    ]
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 0  # l0
    assert len(ss_tables_levels[0]) == 1  # l1
    assert ss_tables_levels[0][0] == sstable3


def test_reconstruct_from_two_flush_events_interspaced_with_compaction_events(sstable_one_block_1,
                                                                              sstable_one_block_2,
                                                                              sstable_one_block_3,
                                                                              sstable_one_block_4,
                                                                              empty_manifest):
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
    manifest = Manifest(events=events, nb_levels=3, file=empty_manifest)

    # WHEN
    all_ss_tables_levels = manifest.reconstruct_sstables()
    level0 = all_ss_tables_levels[0]
    ss_tables_levels = all_ss_tables_levels[1:]

    # THEN
    assert len(level0) == 0  # l0
    assert len(ss_tables_levels[0]) == 2  # l1
    assert ss_tables_levels[0][1] == sstable3
    assert ss_tables_levels[0][0] == sstable4


def test_flush_events_are_equal(sstable_one_block_1):
    # GIVEN
    flush_event_1 = FlushEvent(sstable=sstable_one_block_1)
    flush_event_2 = FlushEvent(sstable=sstable_one_block_1)

    # WHEN
    are_equal = flush_event_1 == flush_event_2

    # THEN
    assert are_equal is True


def test_flush_events_are_not_equal_if_different_sstables(sstable_one_block_1, sstable_one_block_2):
    # GIVEN
    flush_event_1 = FlushEvent(sstable=sstable_one_block_1)
    flush_event_2 = FlushEvent(sstable=sstable_one_block_2)

    # WHEN/THEN
    assert flush_event_1 != flush_event_2


def test_compaction_events_are_equal(sstable_one_block_1, sstable_one_block_2):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    compaction_event_1 = CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=1)
    compaction_event_2 = CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=1)

    # WHEN
    are_equal = compaction_event_1 == compaction_event_2

    # THEN
    assert are_equal is True


def test_compaction_events_are_not_equal_if_different_attributes(sstable_one_block_1, sstable_one_block_2,
                                                                 sstable_one_block_3, sstable_one_block_4):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstable3 = sstable_one_block_3
    sstable4 = sstable_one_block_4
    compaction_event = CompactionEvent(input_sstables=[sstable1, sstable2], output_sstables=[sstable3], level=1)
    compaction_event_input = CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable3], level=1)
    compaction_event_output = CompactionEvent(input_sstables=[sstable1, sstable2], output_sstables=[sstable4], level=1)
    compaction_event_level = CompactionEvent(input_sstables=[sstable1, sstable2], output_sstables=[sstable3], level=2)

    # WHEN/THEN
    assert compaction_event != compaction_event_input
    assert compaction_event != compaction_event_output
    assert compaction_event != compaction_event_level


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


def test_encode_decode_manifest_record_which_is_a_flush_record(sstable_one_block_1):
    # GIVEN
    sstable = sstable_one_block_1
    flush_event = FlushEvent(sstable=sstable)
    manifest_record = ManifestRecord(event=flush_event)

    # WHEN
    encoded_manifest_record = manifest_record.to_bytes()
    decoded_manifest_record = ManifestRecord.from_bytes(data=encoded_manifest_record)

    # THEN
    assert isinstance(manifest_record.event, FlushEvent)
    assert isinstance(decoded_manifest_record.event, FlushEvent)
    assert manifest_record.event == decoded_manifest_record.event


def test_encode_decode_manifest_record_which_is_a_compaction_record(sstable_one_block_1, sstable_one_block_2,
                                                                    sstable_one_block_3, sstable_one_block_4):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    sstable3 = sstable_one_block_3
    sstable4 = sstable_one_block_4
    compaction_event = CompactionEvent(input_sstables=[sstable1, sstable2],
                                       output_sstables=[sstable3, sstable4],
                                       level=1)
    manifest_record = ManifestRecord(event=compaction_event)

    # WHEN
    encoded_manifest_record = manifest_record.to_bytes()
    decoded_manifest_record = ManifestRecord.from_bytes(data=encoded_manifest_record)

    # THEN
    assert isinstance(manifest_record.event, CompactionEvent)
    assert isinstance(decoded_manifest_record.event, CompactionEvent)
    assert manifest_record.event == decoded_manifest_record.event


def test_encode_decode_header():
    # GIVEN
    header = ManifestHeader(nb_levels=6, levels_ratio=0.10, max_l0_sstables=10,
                            block_size=65_536, max_sstable_size=262_144_000)

    # WHEN
    encoded_header = header.to_bytes()
    decoded_header = ManifestHeader.from_bytes(data=encoded_header)

    # THEN
    assert decoded_header == header


def test_create_manifest_file_from_existing_path_should_raise_an_error(empty_manifest):
    # GIVEN
    path_with_file = empty_manifest.path
    configuration = {
        "nb_levels": 10,
        "levels_ratio": 0.1,
        "max_l0_sstables": 10,
        "max_sstable_size": 1000,
        "block_size": 100,
    }

    # WHEN/THEN
    with pytest.raises(ValueError):
        ManifestFile.create(path=path_with_file, **configuration)


def test_open_manifest_file_from_existing_path_should_not_raise_an_error(empty_manifest):
    # GIVEN
    path_with_file = empty_manifest.path

    # WHEN/THEN
    with does_not_raise():
        ManifestFile.open(path=path_with_file)


def test_create_manifest_file_from_new_path_should_not_raise_an_error(manifest_path_with_no_file):
    # GIVEN
    configuration = {
        "nb_levels": 10,
        "levels_ratio": 0.1,
        "max_l0_sstables": 10,
        "max_sstable_size": 1000,
        "block_size": 100,
    }

    # WHEN/THEN
    with does_not_raise():
        ManifestFile.create(path=manifest_path_with_no_file, **configuration)


def test_open_manifest_file_from_new_path_should_raise_an_error(manifest_path_with_no_file):
    # GIVEN/WHEN/THEN
    with pytest.raises(ValueError):
        ManifestFile.open(path=manifest_path_with_no_file)


def test_encode_flush_event_calls_uses_flush_record_encoding(sstable_four_blocks):
    # GIVEN
    sstable = sstable_four_blocks
    event = FlushEvent(sstable=sstable)
    record = ManifestRecord(event=event)

    # WHEN/THEN
    with mock.patch.object(ManifestFlushRecord, 'to_bytes') as mocked_flush_event_to_bytes:
        # WHEN
        record.to_bytes()

        # THEN
        mocked_flush_event_to_bytes.assert_called_once()


def test_encode_compaction_event_calls_uses_compaction_record_encoding(sstable_one_block_1, sstable_one_block_2):
    # GIVEN
    sstable1 = sstable_one_block_1
    sstable2 = sstable_one_block_2
    event = CompactionEvent(input_sstables=[sstable1], output_sstables=[sstable2], level=0)
    record = ManifestRecord(event=event)

    # WHEN/THEN
    with mock.patch.object(ManifestCompactionRecord, 'to_bytes') as mocked_compaction_event_to_bytes:
        # WHEN
        record.to_bytes()

        # THEN
        mocked_compaction_event_to_bytes.assert_called_once()


def test_can_write_events_to_manifest(empty_manifest, empty_manifest_configuration,
                                      sstable_one_block_1, sstable_one_block_2, sstable_one_block_3):
    #  GIVEN
    manifest_file = empty_manifest
    event1 = FlushEvent(sstable=sstable_one_block_1)
    event2 = CompactionEvent(input_sstables=[sstable_one_block_2], output_sstables=[sstable_one_block_3], level=2)

    # WHEN
    manifest_file.write_event(event=event1)
    manifest_file.write_event(event=event2)

    # THEN
    encoded_header = ManifestHeader(**empty_manifest_configuration).to_bytes()
    encoded_record1 = ManifestRecord(event=event1).to_bytes()
    encoded_record2 = ManifestRecord(event=event2).to_bytes()
    with open(manifest_file.path, "rb") as f:
        data = f.read()
    assert data.startswith(encoded_header)
    assert data == encoded_header + encoded_record1 + encoded_record2


def test_can_decode_manifest_file_with_no_events(empty_manifest, empty_manifest_configuration):
    # GIVEN
    manifest_file = empty_manifest

    # WHEN
    header, events = manifest_file.decode()

    # THEN
    assert events == []
    assert header == ManifestHeader(**empty_manifest_configuration)


def test_can_decode_manifest_file_with_multiple_events(empty_manifest, empty_manifest_configuration,
                                                       sstable_one_block_1, sstable_one_block_2,
                                                       sstable_one_block_3, sstable_one_block_4):
    # GIVEN
    manifest_file = empty_manifest
    events = [
        FlushEvent(sstable=sstable_one_block_1),
        CompactionEvent(input_sstables=[sstable_one_block_2, sstable_one_block_3], output_sstables=[], level=3),
        FlushEvent(sstable=sstable_one_block_4)
    ]
    for event in events:
        manifest_file.write_event(event=event)

    # WHEN
    header, decoded_events = manifest_file.decode()

    # THEN
    assert decoded_events == events
    assert header == ManifestHeader(**empty_manifest_configuration)


def test_build_manifest_from_file(sample_manifest_file_1, events_for_sample_manifest_file_1,
                                  configuration_for_sample_manifest_file_1):
    # GIVEN
    manifest_file = sample_manifest_file_1

    # WHEN
    manifest = Manifest.build(manifest_path=manifest_file.path)

    # THEN
    assert manifest.events == events_for_sample_manifest_file_1
    assert manifest.nb_levels == configuration_for_sample_manifest_file_1["nb_levels"]
    assert manifest.file == manifest_file
