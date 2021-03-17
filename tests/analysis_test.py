import time
from random import random

import numpy as np
from signal_processing_algorithms.e_divisive.change_points import EDivisiveChangePoint

from hunter.analysis import fill_missing, compute_change_points, TTestSignificanceTester
from hunter.performance_test import PerformanceTest, AnalysisOptions


def test_change_point_detection():

    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    test = PerformanceTest("test", time, {"series1": series1, "series2": series2}, {})

    change_points = test.find_change_points()
    assert len(change_points) == 2
    assert change_points[0].index == 4
    assert change_points[0].changes[0].metric == "series2"
    assert change_points[1].index == 6
    assert change_points[1].changes[0].metric == "series1"


def test_change_point_min_magnitude():
    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    test = PerformanceTest("test", time, {"series1": series1, "series2": series2}, {})

    options = AnalysisOptions()
    options.min_magnitude = 0.2
    change_points = test.find_change_points(options)
    assert len(change_points) == 1
    assert change_points[0].index == 6
    assert change_points[0].changes[0].metric == "series1"

    for change_point in change_points:
        for change in change_point.changes:
            assert change.magnitude() >= options.min_magnitude, \
                f"All change points must have magnitude greater than {options.min_magnitude}"


def test_change_point_detection_performance():
    timestamps = range(0, 90)   # 3 months of data
    series = [random() for x in timestamps]

    start_time = time.process_time()
    for run in range(0, 10):    # 10 series
        test = PerformanceTest("test", list(timestamps), {"series": series}, {})
        test.find_change_points()
    end_time = time.process_time()
    assert (end_time - start_time) < 0.5


def test_fill_missing():
    list1 = [None, None, 1.0, 1.2, 0.5]
    list2 = [1.0, 1.2, None, None, 4.3]
    list3 = [1.0, 1.2, 0.5, None, None]
    fill_missing(list1)
    fill_missing(list2)
    fill_missing(list3)
    assert list1 == [1.0, 1.0, 1.0, 1.2, 0.5]
    assert list2 == [1.0, 1.2, 1.2, 1.2, 4.3]
    assert list3 == [1.0, 1.2, 0.5, 0.5, 0.5]


def test_single_series():
    series = [1.02, 0.95, 0.99, 1.00, 1.12, 1.00, 1.01, 0.98, 1.01, 0.96,
              0.50, 0.51, 0.48, 0.48, 0.55, 0.50, 0.49, 0.51, 0.50, 0.49]
    indexes = [c.index for c in compute_change_points(series, window_len=10, max_pvalue=0.0001)]
    assert(indexes == [10])


def test_significance_tester():
    tester = TTestSignificanceTester(0.001)

    series = np.array([1.00, 1.02, 1.05, 0.95, 0.98, 1.00, 1.02, 1.05, 0.95, 0.98])
    cp = tester.change_point(5, series, [0, len(series)])
    assert not tester.is_significant(EDivisiveChangePoint(5), series, [0, len(series)])
    assert 0.99 < cp.pvalue < 1.01

    series = np.array([1.00, 1.02, 1.05, 0.95, 0.98, 0.80, 0.82, 0.85, 0.79, 0.77])
    cp = tester.change_point(5, series, [0, len(series)])
    assert tester.is_significant(EDivisiveChangePoint(5), series, [0, len(series)])
    assert 0.00 < cp.pvalue < 0.001



