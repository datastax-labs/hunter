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


def test_merge_dicts():
    assert merge_dicts({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}
    assert merge_dicts({"c": 1, "b": 1, "a": 1}, {"b": 2, "a": 2}) == {"c": 1, "b": 2, "a": 2}
    assert merge_dicts({"a": 1}, {"a": 2}) == {"a": 2}
    assert merge_dicts({"a": [1, 2]}, {"a": [3]}) == {"a": [1, 2, 3]}
    assert merge_dicts({"a": {1, 2}}, {"a": {2, 3}}) == {"a": {1, 2, 3}}
    assert merge_dicts({"a": {"b": [1, 2]}}, {"a": {"b": [3]}}) == {"a": {"b": [1, 2, 3]}}


def test_dict_list():
    d1 = {"a": 1}
    d2 = {"a": 2}
    d3 = {"b": 3}
    assert merge_dict_list([d1, d2, d3]) == {"a": 2, "b": 3}


def test_interpolate():
    s = "name1:%{NAME_1}, name2:%{NAME_2}"
    assert interpolate(s, {"NAME_1": ["foo"], "NAME_2": ["bar"]}) == ["name1:foo, name2:bar"]
    assert interpolate(s, {"NAME_1": ["foo", "bar"], "NAME_2": ["null"]}) == [
        "name1:foo, name2:null",
        "name1:bar, name2:null",
    ]
