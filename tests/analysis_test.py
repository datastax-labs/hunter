import numpy as np
from signal_processing_algorithms.e_divisive.change_points import EDivisiveChangePoint

from hunter.analysis import TTestSignificanceTester, compute_change_points, fill_missing


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
    series = [
        1.02,
        0.95,
        0.99,
        1.00,
        1.12,
        1.00,
        1.01,
        0.98,
        1.01,
        0.96,
        0.50,
        0.51,
        0.48,
        0.48,
        0.55,
        0.50,
        0.49,
        0.51,
        0.50,
        0.49,
    ]
    indexes = [c.index for c in compute_change_points(series, window_len=10, max_pvalue=0.0001)]
    assert indexes == [10]


def test_significance_tester():
    tester = TTestSignificanceTester(0.001)

    series = np.array([1.00, 1.02, 1.05, 0.95, 0.98, 1.00, 1.02, 1.05, 0.95, 0.98])
    cp = tester.change_point(5, series, [0, len(series)])
    assert not tester.is_significant(EDivisiveChangePoint(5), series, [0, len(series)])
    assert 0.99 < cp.stats.pvalue < 1.01

    series = np.array([1.00, 1.02, 1.05, 0.95, 0.98, 0.80, 0.82, 0.85, 0.79, 0.77])
    cp = tester.change_point(5, series, [0, len(series)])
    assert tester.is_significant(EDivisiveChangePoint(5), series, [0, len(series)])
    assert 0.00 < cp.stats.pvalue < 0.001
