from src.blocks import BlockBuilder


def test_can_build_a_block():
    # GIVEN
    block_builder = BlockBuilder(target_size=100)

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
