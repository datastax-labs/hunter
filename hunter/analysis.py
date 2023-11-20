from dataclasses import dataclass
from typing import Iterable, List, Reversible

import numpy as np
from scipy.stats import ttest_ind_from_stats
from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.base import SignificanceTester
from signal_processing_algorithms.e_divisive.calculators import cext_calculator
from signal_processing_algorithms.e_divisive.change_points import EDivisiveChangePoint
from signal_processing_algorithms.e_divisive.significance_test import (
    QHatPermutationsSignificanceTester,
)


@dataclass
class ComparativeStats:
    """
    Keeps statistics of two series of data and the probability both series
    have the same distribution.
    """

    mean_1: float
    mean_2: float
    std_1: float
    std_2: float
    pvalue: float

    def forward_rel_change(self):
        """Relative change from left to right"""
        return self.mean_2 / self.mean_1 - 1.0

    def backward_rel_change(self):
        """Relative change from right to left"""
        return self.mean_1 / self.mean_2 - 1.0

    def change_magnitude(self):
        """Maximum of absolutes of rel_change and rel_change_reversed"""
        return max(abs(self.forward_rel_change()), abs(self.backward_rel_change()))


@dataclass
class ChangePoint:
    index: int
    stats: ComparativeStats


class ExtendedSignificanceTester(SignificanceTester):
    """
    Adds capability of exposing the means and deviations of both sides of the split
    and the pvalue (strength) of the split.
    """

    pvalue: float

    def change_point(self, index: int, series: np.ndarray, windows: Iterable[int]) -> ChangePoint:
        """
        Computes properties of the change point if the change point gets
        inserted at the given index into the series array.
        """
        ...

    def compare(self, left: np.ndarray, right: np.ndarray) -> ComparativeStats:
        """
        Compares two sets of points for similarity / difference.
        Computes basic stats and probability both sets come from the same distribution/
        """
        ...

    @staticmethod
    def find_window(candidate: int, window_endpoints: Reversible[int]) -> (int, int):
        start: int = next((x for x in reversed(window_endpoints) if x < candidate), None)
        end: int = next((x for x in window_endpoints if x > candidate), None)
        return start, end

    def is_significant(
        self, candidate: EDivisiveChangePoint, series: np.ndarray, windows: Iterable[int]
    ) -> bool:
        try:
            cp = self.change_point(candidate.index, series, windows)
            return cp.stats.pvalue <= self.pvalue
        except ValueError:
            return False


class TTestSignificanceTester(ExtendedSignificanceTester):
    """
    Uses two-sided Student's T-test to decide if a candidate change point
    splits the series into pieces that are significantly different from each other.
    This test is good if the data between the change points have normal distribution.
    It works well even with tiny numbers of points (<10).
    """

    def __init__(self, pvalue: float):
        self.pvalue = pvalue

    def change_point(
        self, index: int, series: np.ndarray, window_endpoints: Reversible[int]
    ) -> ChangePoint:

        (start, end) = self.find_window(index, window_endpoints)
        left = series[start:index]
        right = series[index:end]
        stats = self.compare(left, right)
        return ChangePoint(index, stats)

    def compare(self, left: np.ndarray, right: np.ndarray) -> ComparativeStats:
        if len(left) == 0 or len(right) == 0:
            raise ValueError

        mean_l = np.mean(left)
        mean_r = np.mean(right)
        std_l = np.std(left) if len(left) >= 2 else 0.0
        std_r = np.std(right) if len(right) >= 2 else 0.0

        if len(left) + len(right) > 2:
            (_, p) = ttest_ind_from_stats(
                mean_l, std_l, len(left), mean_r, std_r, len(right), alternative="two-sided"
            )
        else:
            p = 1.0
        return ComparativeStats(mean_l, mean_r, std_l, std_r, p)


def fill_missing(data: List[float]):
    """
    Forward-fills None occurrences with nearest previous non-None values.
    Initial None values are back-filled with the nearest future non-None value.
    """
    prev = None
    for i in range(len(data)):
        if data[i] is None and prev is not None:
            data[i] = prev
        prev = data[i]

    prev = None
    for i in reversed(range(len(data))):
        if data[i] is None and prev is not None:
            data[i] = prev
        prev = data[i]


def merge(
    change_points: List[ChangePoint], series: np.array, max_pvalue: float, min_magnitude: float
) -> List[ChangePoint]:
    """
    Removes weak change points recursively going bottom-up
    until we get only high-quality change points
    that meet the P-value and rel_change criteria.

    Parameters:
        :param max_pvalue: maximum accepted pvalue
        :param min_magnitude: minimum accepted relative change
    """

    tester = TTestSignificanceTester(max_pvalue)
    while change_points:

        # Select the change point with weakest unacceptable P-value
        # If all points have acceptable P-values, select the change-point with
        # the least relative change:
        weakest_cp = max(change_points, key=lambda c: c.stats.pvalue)
        if weakest_cp.stats.pvalue < max_pvalue:
            weakest_cp = min(change_points, key=lambda c: c.stats.change_magnitude())
            if weakest_cp.stats.change_magnitude() > min_magnitude:
                return change_points

        # Remove the point from the list
        weakest_cp_index = change_points.index(weakest_cp)
        del change_points[weakest_cp_index]

        # We can't continue yet, because by removing a change_point
        # the adjacent change points changed their properties.
        # Recompute the adjacent change point stats:
        window_endpoints = [0] + [cp.index for cp in change_points] + [len(series)]

        def recompute(index: int):
            if index < 0 or index >= len(change_points):
                return
            cp = change_points[index]
            change_points[index] = tester.change_point(cp.index, series, window_endpoints)

        recompute(weakest_cp_index)
        recompute(weakest_cp_index + 1)

    return change_points


def split(series: np.array, window_len: int = 30, max_pvalue: float = 0.001) -> List[ChangePoint]:
    """
    Finds change points by splitting the series top-down.

    Internally it uses the EDivisive algorithm from mongodb-signal-processing
    that recursively splits the series in a way to maximize some measure of
    dissimilarity (denoted qhat) between the chunks.
    Splitting happens as long as the dissimilarity is statistically significant.

    Unfortunately this algorithms has a few downsides:
    - the complexity is O(n^2), where n is the length of the series
    - if there are too many change points and too much data, the change points in the middle
      of the series may be missed

    This function tries to address these issues by invoking EDivisive on smaller
    chunks (windows) of the input data instead of the full series and then merging the results.
    Each window should be large enough to contain enough points to detect a change-point.
    Consecutive windows overlap so that we won't miss changes happening between them.
    """
    assert "Window length must be at least 2", window_len >= 2
    start = 0
    step = int(window_len / 2)
    indexes = []
    tester = TTestSignificanceTester(max_pvalue)
    while start < len(series):
        end = min(start + window_len, len(series))
        calculator = cext_calculator
        algo = EDivisive(seed=None, calculator=calculator, significance_tester=tester)
        pts = algo.get_change_points(series[start:end])
        new_indexes = [p.index + start for p in pts]
        new_indexes.sort()
        last_new_change_point_index = next(iter(new_indexes[-1:]), 0)
        start = max(last_new_change_point_index, start + step)
        indexes += new_indexes

    window_endpoints = [0] + indexes + [len(series)]
    return [tester.change_point(i, series, window_endpoints) for i in indexes]


def compute_change_points_orig(series: np.array, max_pvalue: float = 0.001) -> List[ChangePoint]:
    calculator = cext_calculator
    tester = QHatPermutationsSignificanceTester(calculator, pvalue=max_pvalue, permutations=100)
    algo = EDivisive(seed=None, calculator=calculator, significance_tester=tester)
    pts = algo.get_change_points(series)
    indexes = [p.index for p in pts]
    window_endpoints = [0] + indexes + [len(series)]
    return [tester.change_point(i, series, window_endpoints) for i in indexes]


def compute_change_points(
    series: np.array, window_len: int = 50, max_pvalue: float = 0.001, min_magnitude: float = 0.05
) -> List[ChangePoint]:
    change_points = split(series, window_len, max_pvalue * 10)
    return merge(change_points, series, max_pvalue, min_magnitude)
