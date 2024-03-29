from src.blocks import DataBlockBuilder, DataBlock


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
