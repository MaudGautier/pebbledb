from typing import Iterator, Any


def merge_iterators(iterators: list[Iterator[Any]]) -> Iterator[Any]:
    has_no_value = object()

    def get_next(iterator: Iterator[Any]):
        try:
            return next(iterator)
        except StopIteration:
            return has_no_value

    next_values = []
    for i, iterator in enumerate(iterators):
        next_value = get_next(iterator)
        next_values.append((next_value, i))

    while len(remaining_values := [value for value in next_values if value[0] is not has_no_value]):
        value_to_yield, idx = min(remaining_values)
        yield value_to_yield
        next_value = get_next(iterators[idx])
        next_values[idx] = (next_value, idx)
