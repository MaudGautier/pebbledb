from src.utils import merge_iterators


def test_merge_iterators():
    # GIVEN
    iterator1 = (i for i in range(5) if i % 2 == 0)
    iterator2 = (i for i in range(5) if i % 2 == 1)

    # WHEN
    merged_iterator = merge_iterators([iterator1, iterator2])

    # THEN
    expected_values = [i for i in range(5)]
    assert list(merged_iterator) == expected_values

