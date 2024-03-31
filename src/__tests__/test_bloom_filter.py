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

