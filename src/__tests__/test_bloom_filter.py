from src.bloom_filter import BloomFilter


def test_lookups():
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=4, nb_hash_functions=3)
    keys = ["foo", "bar", "baz"]
    missing_keys = ["not_in_bloom_filter", "missing"]

    # WHEN
    for key in keys:
        bloom_filter.add(key=key)

    # THEN
    for key in keys:
        assert bloom_filter.may_contain(key=key) is True

    # They could be true, but in this case we know they return False (probabilistic)
    for key in missing_keys:
        assert bloom_filter.may_contain(key=key) is False


def test_build_from_keys():
    # GIVEN/WHEN
    bloom_filter = BloomFilter.build_from_keys_and_fp_rate(["key1", "key2"], 0.001)

    # THEN
    assert bloom_filter.may_contain("key1") is True
    assert bloom_filter.may_contain("key2") is True


def test_encode():
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=1, nb_hash_functions=2)
    bloom_filter.add("key1")
    bloom_filter.add("key2")
    bloom_filter.add("key3")

    # WHEN
    encoded_bloom_filter = bloom_filter.to_bytes()

    # THEN
    encoded_bits = b'3'  # Value obtained when hashing all keys
    encoded_nb_hash = b'\x02'
    assert encoded_bloom_filter == encoded_bits + encoded_nb_hash


def test_encode_with_multiple_bytes():
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=3, nb_hash_functions=2)
    bloom_filter.add("key1")
    bloom_filter.add("key2")
    bloom_filter.add("key3")

    # WHEN
    encoded_bloom_filter = bloom_filter.to_bytes()

    # THEN
    encoded_bits = b'\x003\x10'  # Value obtained when hashing all keys
    encoded_nb_hash = b'\x02'
    assert encoded_bloom_filter == encoded_bits + encoded_nb_hash


def test_encode_decode():
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=1, nb_hash_functions=2)
    bloom_filter.add("key1")
    bloom_filter.add("key2")
    bloom_filter.add("key3")
    encoded_bloom_filter = bloom_filter.to_bytes()

    # WHEN
    decoded_bloom_filter = BloomFilter.from_bytes(data=encoded_bloom_filter)

    # THEN
    assert bloom_filter.nb_hash_functions == decoded_bloom_filter.nb_hash_functions
    assert bloom_filter.bits_size == decoded_bloom_filter.bits_size
    assert bloom_filter.bits == decoded_bloom_filter.bits
    assert bloom_filter.nb_bytes == decoded_bloom_filter.nb_bytes


def test_encode_decode_with_multiple_bytes():
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=3, nb_hash_functions=2)
    bloom_filter.add("key1")
    bloom_filter.add("key2")
    bloom_filter.add("key3")
    encoded_bloom_filter = bloom_filter.to_bytes()

    # WHEN
    decoded_bloom_filter = BloomFilter.from_bytes(data=encoded_bloom_filter)

    # THEN
    assert bloom_filter.nb_hash_functions == decoded_bloom_filter.nb_hash_functions
    assert bloom_filter.bits_size == decoded_bloom_filter.bits_size
    assert bloom_filter.bits == decoded_bloom_filter.bits
    assert bloom_filter.nb_bytes == decoded_bloom_filter.nb_bytes


def test_bloom_filters_are_equal():
    # GIVEN
    keys = ["key1", "key2", "key3"]
    bloom_filter1 = BloomFilter(nb_bytes=8, nb_hash_functions=3)
    bloom_filter2 = BloomFilter(nb_bytes=8, nb_hash_functions=3)
    for key in keys:
        bloom_filter1.add(key)
        bloom_filter2.add(key)

    # WHEN
    are_equal = bloom_filter1 == bloom_filter2

    # THEN
    assert are_equal is True


def test_bloom_filters_are_not_equal_if_different_number_of_hash_functions():
    # GIVEN
    keys = ["key1", "key2", "key3"]
    bloom_filter1 = BloomFilter(nb_bytes=8, nb_hash_functions=3)
    bloom_filter2 = BloomFilter(nb_bytes=8, nb_hash_functions=4)
    for key in keys:
        bloom_filter1.add(key)
        bloom_filter2.add(key)

    # WHEN
    are_equal = bloom_filter1 == bloom_filter2

    # THEN
    assert are_equal is False


def test_bloom_filters_are_not_equal_if_different_keys():
    # GIVEN
    keys = ["key1", "key2", "key3"]
    bloom_filter1 = BloomFilter(nb_bytes=8, nb_hash_functions=3)
    bloom_filter2 = BloomFilter(nb_bytes=8, nb_hash_functions=4)
    for key in keys:
        bloom_filter1.add(key)
    for key in keys[:2]:
        bloom_filter2.add(key)

    # WHEN
    are_equal = bloom_filter1 == bloom_filter2

    # THEN
    assert are_equal is False
