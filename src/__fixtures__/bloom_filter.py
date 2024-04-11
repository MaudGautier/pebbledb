import pytest

from src.bloom_filter import BloomFilter


@pytest.fixture
def simple_bloom_filter():
    bloom_filter = BloomFilter(nb_bytes=10, nb_hash_functions=3)
    keys = ["key1", "key2", "key3", "key5", "key9"]
    for key in keys:
        bloom_filter.add(key)

    return bloom_filter

@pytest.fixture
def simple_bloom_filter_2():
    bloom_filter = BloomFilter(nb_bytes=10, nb_hash_functions=3)
    keys = ["key1", "key2", "key3"]
    for key in keys:
        bloom_filter.add(key)

    return bloom_filter
