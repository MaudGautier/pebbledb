from typing import Iterator, Any
from heapq import heappush, heappop


def merge_iterators(iterators: list[Iterator[Any]]) -> Iterator[Any]:
    has_no_value = object()

    def get_next(iterator: Iterator[Any]):
        try:
            return next(iterator)
        except StopIteration:
            return has_no_value

    heap = []
    for i, iterator in enumerate(iterators):
        next_value = get_next(iterator)
        heappush(heap, (next_value, i))

    while len(heap):
        value, idx = heappop(heap)
        yield value
        next_value = get_next(iterators[idx])
        if next_value is has_no_value:
            continue
        heappush(heap, (next_value, idx))
