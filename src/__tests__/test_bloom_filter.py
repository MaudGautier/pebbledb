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
    # GIVEN
    bloom_filter = BloomFilter(nb_bytes=1, nb_hash_functions=2)

    # WHEN
    bloom_filter.build_from_keys(["key1", "key2"])

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
    encoded_bits = b'9'  # Value obtained when hashing all keys
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
    assert bloom_filter.hash_functions == decoded_bloom_filter.hash_functions
    assert bloom_filter.bits_size == decoded_bloom_filter.bits_size
    assert bloom_filter.bits == decoded_bloom_filter.bits
    assert bloom_filter.nb_bytes == decoded_bloom_filter.nb_bytes
