from hunter.util import *


def test_merge_sorted():
    assert merge_sorted([]) == []
    assert merge_sorted([[]]) == []
    assert merge_sorted([[1]]) == [1]
    assert merge_sorted([[1], [1]]) == [1]
    assert merge_sorted([[1], [2]]) == [1, 2]
    assert merge_sorted([[2], [1]]) == [1, 2]
    assert merge_sorted([[3, 2, 1], []]) == [1, 2, 3]
    assert merge_sorted([[1, 3, 4], [1, 2]]) == [1, 2, 3, 4]


def test_remove_common_prefix():
    assert remove_common_prefix([""]) == [""]
    assert remove_common_prefix(["foo"]) == ["foo"]
    assert remove_common_prefix(["foo", "bar"]) == ["foo", "bar"]
    assert remove_common_prefix(["foo.1", "foo.2"]) == ["1", "2"]
    assert remove_common_prefix(["foo.bar.1", "foo.bar.2", "foo.3"]) == ["bar.1", "bar.2", "3"]
    assert remove_common_prefix(["a/b", "a/c"], "/") == ["b", "c"]


def test_insert_multiple():
    assert insert_multiple(["a", "b", "c"], ["-"], []) == ["a", "b", "c"]
    assert insert_multiple(["a", "b", "c"], ["-"], [1]) == ["a", "-", "b", "c"]
    assert insert_multiple(["a", "b", "c"], ["0", "1", "2"], [0, 1, 2]) == [
        "0",
        "a",
        "1",
        "b",
        "2",
        "c",
    ]


def test_sliding_window():
    collection = [0, 1]
    iter = sliding_window(collection, 3)
    assert next(iter, None) is None

    collection = [0, 1, 2, 3, 4]
    iter = sliding_window(collection, 3)
    assert next(iter) == (0, 1, 2)
    assert next(iter) == (1, 2, 3)
    assert next(iter) == (2, 3, 4)
    assert next(iter, None) is None
