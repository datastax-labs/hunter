import time
from random import random

from hunter.series import Series, AnalysisOptions, compare


def test_change_point_detection():
    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    test = Series("test", time, {"series1": series1, "series2": series2}, {})

    change_points = test.all_change_points()
    assert len(change_points) == 2
    assert change_points[0].index == 4
    assert change_points[0].changes[0].metric == "series2"
    assert change_points[1].index == 6
    assert change_points[1].changes[0].metric == "series1"


def test_change_point_min_magnitude():
    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    test = Series("test", time, {"series1": series1, "series2": series2}, {})

    options = AnalysisOptions()
    options.min_magnitude = 0.2
    change_points = test.all_change_points(options)
    assert len(change_points) == 1
    assert change_points[0].index == 6
    assert change_points[0].changes[0].metric == "series1"

    for change_point in change_points:
        for change in change_point.changes:
            assert (
                change.magnitude() >= options.min_magnitude
            ), f"All change points must have magnitude greater than {options.min_magnitude}"


def test_change_point_detection_performance():
    timestamps = range(0, 90)  # 3 months of data
    series = [random() for x in timestamps]

    start_time = time.process_time()
    for run in range(0, 10):  # 10 series
        test = Series("test", list(timestamps), {"series": series}, {})
        test.all_change_points()
    end_time = time.process_time()
    assert (end_time - start_time) < 0.5


def test_get_stable_range():
    series_1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series_2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series_1)))
    test = Series("test", time, {"series1": series_1, "series2": series_2}, {})

    change_points_1 = test.change_points("series1")

    assert test.get_stable_range(0, change_points_1) == (0, 6)
    assert test.get_stable_range(1, change_points_1) == (0, 6)
    assert test.get_stable_range(5, change_points_1) == (0, 6)
    assert test.get_stable_range(6, change_points_1) == (6, len(series_1))
    assert test.get_stable_range(7, change_points_1) == (6, len(series_1))
    assert test.get_stable_range(10, change_points_1) == (6, len(series_1))

    change_points_2 = test.change_points("series2")
    assert test.get_stable_range(0, change_points_2) == (0, 4)
    assert test.get_stable_range(1, change_points_2) == (0, 4)
    assert test.get_stable_range(3, change_points_2) == (0, 4)


def test_compare():
    series_1 = [1.02, 0.95, 0.99, 1.00, 1.04, 1.02, 0.50, 0.51, 0.48, 0.48, 0.53]
    series_2 = [2.02, 2.03, 2.01, 2.04, 0.51, 0.49, 0.51, 0.49, 0.48, 0.52, 0.50]
    time = list(range(len(series_1)))
    test_1 = Series("test_1", time, {"data": series_1}, {})
    test_2 = Series("test_2", time, {"data": series_2}, {})

    stats = compare(test_1, None, test_2, None).stats["data"]
    assert stats.pvalue > 0.5  # tails are almost the same
    assert 0.48 < stats.mean_1 < 0.52
    assert 0.48 < stats.mean_2 < 0.52

    stats = compare(test_1, 0, test_2, 0).stats["data"]
    assert stats.pvalue < 0.01  # beginnings are different
    assert 0.98 < stats.mean_1 < 1.02
    assert 2.00 < stats.mean_2 < 2.03

    stats = compare(test_1, 5, test_2, 10).stats["data"]
    assert stats.pvalue < 0.01
    assert 0.98 < stats.mean_1 < 1.02
    assert 0.49 < stats.mean_2 < 0.51


def test_compare_single_point():
    series_1 = [1.02, 0.95, 0.99, 1.00, 1.04, 1.02, 0.50, 0.51, 0.48, 0.48, 0.53]
    series_2 = [0.51]
    series_3 = [0.99]

    test_1 = Series("test_1", list(range(len(series_1))), {"data": series_1}, {})
    test_2 = Series("test_2", [1], {"data": series_2}, {})
    test_3 = Series("test_3", [1], {"data": series_3}, {})

    stats = compare(test_1, None, test_2, None).stats["data"]
    assert stats.pvalue > 0.5

    stats = compare(test_1, 5, test_3, None).stats["data"]
    assert stats.pvalue > 0.5

    stats = compare(test_1, None, test_3, None).stats["data"]
    assert stats.pvalue < 0.01
