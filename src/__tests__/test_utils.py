from src.record import Record
from src.utils import merge_iterators, filter_duplicate_keys


def test_merge_iterators():
    # GIVEN
    iterator1 = (i for i in range(5) if i % 2 == 0)
    iterator2 = (i for i in range(5) if i % 2 == 1)

    # WHEN
    merged_iterator = merge_iterators([iterator1, iterator2])

    # THEN
    expected_values = [i for i in range(5)]
    assert list(merged_iterator) == expected_values


def test_merge_iterators_with_identical_values():
    """If two iterators have the same key, it should select the value from the first iterator and ignore others"""
    # GIVEN
    iterator1_items = [
        ("A", "A1"),
        ("B", "B1"),
        ("D", "D1")
    ]
    iterator2_items = [
        ("A", "A2"),
        ("C", "C2"),
        ("D", "D2"),
        ("E", "E2"),
    ]
    iterator1 = (Record(key=item[0], value=item[1]) for item in iterator1_items)
    iterator2 = (Record(key=item[0], value=item[1]) for item in iterator2_items)

    # WHEN
    merged_iterator_with_duplicates = merge_iterators(iterators=[iterator1, iterator2])
    merged_iterator = filter_duplicate_keys(iterator=merged_iterator_with_duplicates)

    # THEN
    expected_items = [
        Record(key="A", value="A1"),
        Record(key="B", value="B1"),
        Record(key="C", value="C2"),
        Record(key="D", value="D1"),
        Record(key="E", value="E2")
    ]
    assert list(merged_iterator) == expected_items


def test_merge_iterators_when_one_empty():
    # GIVEN
    iterator_with_one_item = (i for i in range(1))
    empty_iterator = (i for i in range(0) if i % 2 == 1)

    # WHEN
    merged_iterator = merge_iterators([empty_iterator, iterator_with_one_item])

    # THEN
    expected_values = [0]
    assert list(merged_iterator) == expected_values
