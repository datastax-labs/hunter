import datetime
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import List, TypeVar, Optional

import dateparser
from pytz import UTC


def remove_prefix(text: str, prefix: str) -> str:
    """
    Strips prefix of a string. If the string doesn't start with the given
    prefix, returns the original string unchanged.
    """
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


T = TypeVar('T')


def merge_sorted(lists: List[List[T]]) -> List[T]:
    """
    Merges multiple sorted lists into a sorted list that contains
    only distinct items from the source lists.
    Current implementation uses sorting, so it is not very efficient for
    very large lists.

    Example:
        - input:  [[0, 1, 2, 4, 5], [0, 1, 2, 3, 5]]
        - output: [0, 1, 2, 3, 4, 5]
    """
    output = set()
    for l in lists:
        for item in l:
            output.add(item)

    output = list(output)
    output.sort()
    return output


def remove_common_prefix(names: List[str], sep: str = ".") \
        -> List[str]:
    """
    """

    if len(names) == 0:
        return names

    split_names = [name.split(sep) for name in names]
    min_len = min(len(components) for components in split_names)

    def are_same(index: int) -> bool:
        return all(c[index] == split_names[0][index] for c in split_names)

    prefix_len = 0
    while prefix_len + 1 < min_len and are_same(prefix_len):
        prefix_len += 1

    return [sep.join(components[prefix_len:]) for components in split_names]


def eprint(*args, **kwargs):
    """Prints to stdandard error"""
    print(*args, file=sys.stderr, **kwargs)


def format_timestamp(ts: int) -> str:
    return datetime\
        .fromtimestamp(ts, tz=UTC)\
        .strftime("%Y-%m-%d %H:%M:%S %z")


def insert_multiple(col: List[T], new_items: List[T], positions: List[int]) -> List[T]:
    """Inserts an item into a collection at given positions"""
    result = []
    positions = set(positions)
    new_items_iter = iter(new_items)
    for i, x in enumerate(col):
        if i in positions:
            result.append(next(new_items_iter))
        result.append(x)
    return result


@dataclass
class DateFormatError(ValueError):
    message: str


def parse_datetime(date: Optional[str]) -> Optional[datetime]:
    """
    Converts a human-readable string into a datetime object.
    Accepts many formats and many languages, see dateparser package.
    Raises DataFormatError if the input string format hasn't been recognized.
    """
    if date is None:
        return None
    parsed: datetime = dateparser.parse(
        date,
        settings={'RETURN_AS_TIMEZONE_AWARE': True})
    if parsed is None:
        raise DateFormatError(f"Invalid datetime value: {date}")
    return parsed


def sliding_window(iterable, size):
    """
    Returns an iterator which represents a sliding window over the given
    collection. `size` denotes the size of the window. If the collection length
    is less than the size, no items are yielded.
    """
    iterable = iter(iterable)
    window = deque(islice(iterable, size), maxlen=size)
    for item in iterable:
        yield tuple(window)
        window.append(item)
    if len(window) == size:
        # needed because if iterable was already empty before the `for`,
        # then the window would be yielded twice.
        yield tuple(window)
