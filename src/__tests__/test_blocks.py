from src.blocks import DataBlockBuilder, DataBlock, MetaBlock
from src.record import Record


def test_block_builder_buffer_and_offsets():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=100)

    # WHEN
    block_builder.add(key="key1", value=b'value1')
    block_builder.add(key="key2", value=b'value2')
    block_builder.add(key="key3", value=b'value3')
    block_builder.add(key="key4", value=b'value4')

    # THEN
    record_size = len("keyN") + len(b"valueN") + 4 + 4
    expected_offsets = [i * record_size for i in range(4)]
    assert block_builder.offsets == expected_offsets
    expected_data_chunks = [
        b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1',
        b'\x04\x00\x00\x00key2\x06\x00\x00\x00value2',
        b'\x04\x00\x00\x00key3\x06\x00\x00\x00value3',
        b'\x04\x00\x00\x00key4\x06\x00\x00\x00value4',
    ]
    assert block_builder.data_buffer[:4 * record_size] == b''.join(expected_data_chunks)


def test_block_builder_returns_false_when_too_big():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=20)

    # WHEN
    add_key1_return = block_builder.add(key="key1", value=b'value1')
    add_key2_return = block_builder.add(key="key2", value=b'value2')

    # THEN
    assert add_key1_return is True
    assert add_key2_return is False
    assert block_builder.offsets == [0]
    assert block_builder.data_buffer == b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x00\x00'


def test_encode_data_block():
    # GIVEN
    data = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x04\x00\x00\x00key2\x06\x00\x00\x00value2'
    block = DataBlock(data=data, offsets=[0, 18])
    assert block.number_records == 2

    # WHEN
    encoded_block = block.to_bytes()

    # THEN
    encoded_0 = b'\x00\x00'
    encoded_18 = b'\x12\x00'
    encoded_2 = b'\x02\x00'
    expected_encoded_offsets = encoded_0 + encoded_18
    expected_encoded_nb_elements = encoded_2
    assert encoded_block == data + expected_encoded_offsets + expected_encoded_nb_elements


def test_decode_data_block():
    # GIVEN
    data = b'\x04\x00\x00\x00key1\x06\x00\x00\x00value1\x04\x00\x00\x00key2\x06\x00\x00\x00value2'
    offsets = [0, 18]
    block = DataBlock(data=data, offsets=offsets)

    # WHEN
    encoded_block = block.to_bytes()
    decoded_block = DataBlock.from_bytes(encoded_block)

    # THEN
    assert decoded_block.number_records == 2
    assert decoded_block.data == data
    assert decoded_block.offsets == [0, 18]


def test_encode_meta_block():
    # GIVEN
    block = MetaBlock(first_key="first_key", last_key="last_key", offset=100)

    # WHEN
    encoded_block = block.to_bytes()

    # THEN
    first_key_size = b'\t\x00'  # encoded 14
    encoded_first_key = b'first_key'
    last_key_size = b'\x08\x00'  # encoded 13
    encoded_last_key = b'last_key'
    encoded_offset = b'd\x00\x00\x00'  # encoded 100
    assert encoded_block == first_key_size + encoded_first_key + last_key_size + encoded_last_key + encoded_offset


def test_decode_meta_block():
    # GIVEN
    encoded_meta_block = b'\t\x00first_key\x08\x00last_keyd\x00\x00\x00'

    # WHEN
    decoded_block = MetaBlock.from_bytes(encoded_meta_block)

    # THEN
    assert decoded_block.first_key == "first_key"
    assert decoded_block.last_key == "last_key"
    assert decoded_block.offset == 100


def test_get_record():
    # GIVEN
    block_builder = DataBlockBuilder(target_size=100)
    kv_pairs = [
        ("key1", b'value1'),
        ("key2", b'value2'),
        ("key3", b'value3'),
        ("key4", b'value4'),
    ]
    for key, value in kv_pairs:
        block_builder.add(key=key, value=value)
    block = block_builder.create_block()

    # WHEN
    record_key1 = block.get("key1")
    record_key2 = block.get("key2")
    record_key3 = block.get("key3")
    record_key4 = block.get("key4")
    record_missing_key = block.get("missing_key")

    # THEN
    assert record_key1 == Record(key=kv_pairs[0][0], value=kv_pairs[0][1])
    assert record_key2 == Record(key=kv_pairs[1][0], value=kv_pairs[1][1])
    assert record_key3 == Record(key=kv_pairs[2][0], value=kv_pairs[2][1])
    assert record_key4 == Record(key=kv_pairs[3][0], value=kv_pairs[3][1])
    assert record_missing_key is None
