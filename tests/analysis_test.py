from random import random
import time

from hunter.analysis import PerformanceLog, fill_missing, compute_change_points
import numpy as np


def test_change_point_detection():

    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    log = PerformanceLog("test", time, {"series1": series1, "series2": series2}, {})

    change_points = log.find_change_points()
    assert len(change_points) == 2
    assert change_points[0].index == 4
    assert change_points[0].changes[0].metric == "series2"
    assert change_points[1].index == 6
    assert change_points[1].changes[0].metric == "series1"


def test_change_point_detection_performance():
    timestamps = range(0, 90)   # 3 months of data
    series = [random() for x in timestamps]

    start_time = time.process_time()
    for run in range(0, 10):    # 10 series
        log = PerformanceLog("test", list(timestamps), {"series": series}, {})
        log.find_change_points()
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
    indexes = compute_change_points(series, window_len=10, pvalue=0.05)
    assert(indexes == [10])


