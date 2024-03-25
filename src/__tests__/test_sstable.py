from src.blocks import Block
from src.sstable import SSTableBuilder, SSTable


def test_add_record_to_current_block():
    # GIVEN
    sstable_builder = SSTableBuilder(sstable_size=150, block_size=50)

    # WHEN
    kv_pairs = [
        ("key1", b'value1'),
        ("key2", b'value2'),
    ]
    for key, value in kv_pairs:
        sstable_builder.add(key=key, value=value)

    # THEN
    assert sstable_builder.current_buffer_position == 0
    assert sstable_builder.data_block_offsets == []
    assert sstable_builder.data_buffer == bytearray(150)


def test_adding_record_to_new_block_updates_buffer():
    # GIVEN
    sstable_builder = SSTableBuilder(sstable_size=150, block_size=50)

    # WHEN
    kv_pairs = [
        ("key1", b'value1'),
        ("key2", b'value2'),
        ("key3", b'value3'),
    ]
    for key, value in kv_pairs:
        sstable_builder.add(key=key, value=value)

    # THEN
    record_size = len("keyN") + len(b"valueN") + 4 + 4
    record_index_size = 2  # Number of bytes for a "H" integer (Blocks)
    nb_records = 2
    nb_records_size = 2  # Number of bytes for a "H" integer (Blocks)
    block_index_size = nb_records * record_index_size + nb_records_size
    assert sstable_builder.current_buffer_position == 2 * record_size + block_index_size
    assert sstable_builder.data_block_offsets == [0]


def test_encode_sstable():
    # GIVEN
    data1 = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x04\x00\x00\x00key2\x06\x00\x00\x00value2'
    block1 = Block(data=data1, offsets=[0, 18])
    encoded_block1 = block1.to_bytes()
    data2 = b'\x04\x00\x00\x00key3\x06\x00\x00\x00value3'
    block2 = Block(data=data2, offsets=[0])
    encoded_block2 = block2.to_bytes()
    data = encoded_block1 + encoded_block2
    sstable = SSTable(data=data, offsets=[0, 42])

    # WHEN
    encoded_sstable = sstable.to_bytes()

    # THEN
    encoded_0 = b'\x00\x00\x00\x00'
    encoded_42 = b'*\x00\x00\x00'
    expected_encoded_offsets = encoded_0 + encoded_42
    encoded_2 = b'\x02\x00\x00\x00'
    expected_encoded_nb_elements = encoded_2
    assert encoded_sstable == data + expected_encoded_offsets + expected_encoded_nb_elements


def test_decode_sstable():
    # GIVEN
    data = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x04\x00\x00\x00key2\x06\x00\x00\x00value2\x00\x00\x12\x00\x02\x00\x04\x00\x00\x00key3\x06\x00\x00\x00value3\x00\x00\x01\x00\x00\x00\x00\x00*\x00\x00\x00\x02\x00\x00\x00'
    offsets = [0, 42]
    sstable = SSTable(data=data, offsets=offsets)

    # WHEN
    encoded_sstable = sstable.to_bytes()
    decoded_sstable = SSTable.from_bytes(encoded_sstable)

    # THEN
    assert decoded_sstable.number_data_blocks == 2
    assert decoded_sstable.data == data
    assert decoded_sstable.offsets == [0, 42]
