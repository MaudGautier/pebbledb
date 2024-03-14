from typing import Iterator, Any
from heapq import heappush, heappop

from src.record import Record


def merge_iterators(iterators: list[Iterator[Any]]) -> Iterator[Any]:
    no_item = object()
    heap = []

    def get_next(iterator: Iterator[Any]):
        try:
            return next(iterator)
        except StopIteration:
            return no_item

    def try_push_iterator(iterator_index: int):
        next_item = get_next(iterator=iterators[iterator_index])
        if next_item is no_item:
            return
        heappush(heap, (next_item, iterator_index))

    for i in range(len(iterators)):
        try_push_iterator(i)

    while len(heap):
        value, i = heappop(heap)
        yield value
        try_push_iterator(i)


def filter_duplicate_keys(iterator: Iterator[Record]) -> Iterator[Record]:
    previous_item = None
    for item in iterator:
        if previous_item is not None and item.is_duplicate(previous_item):
            continue
        yield item
        previous_item = item
