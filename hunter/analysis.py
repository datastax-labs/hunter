from dataclasses import dataclass
from typing import Iterable
from typing import List

import numpy as np
from scipy.stats import mannwhitneyu
from scipy.stats import ttest_ind_from_stats
from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.base import SignificanceTester
from signal_processing_algorithms.e_divisive.calculators import cext_calculator
from signal_processing_algorithms.e_divisive.change_points import EDivisiveChangePoint


@dataclass
class ChangePoint:
    index: int
    mean_l: float
    mean_r: float
    std_l: float
    std_r: float
    pvalue: float

    def rel_change(self):
        return self.mean_r / self.mean_l - 1.0


class ExtendedSignificanceTester(SignificanceTester):
    """
    Adds capability of exposing the means and deviations of both sides of the split
    and the pvalue (strength) of the split.
    """
    pvalue: float

    def change_point(
            self,
            index: int,
            series: np.ndarray,
            windows: Iterable[int]) -> ChangePoint:
        ...

    @staticmethod
    def find_window(candidate: int, windows: Iterable[int]) -> (int, int):
        start: int = next((x for x in reversed(windows) if x < candidate), None)
        end: int = next((x for x in windows if x > candidate), None)
        return start, end

    def is_significant(
            self, candidate: EDivisiveChangePoint, series: np.ndarray, windows: Iterable[int]
    ) -> bool:
        stats = self.change_point(candidate.index, series, windows)
        return stats.pvalue <= self.pvalue


class MannWhitneySignificanceTester(ExtendedSignificanceTester):
    """
    Uses two-sided Mann-Whitney test to decide if a candidate change point
    splits the series into pieces that are significantly different from each other.
    Does not require data to be normally distributed, but doesn't work well if the
    number of points is smaller than 30.
    """
    def __init__(self, pvalue: float):
        self.pvalue = pvalue

    def change_point(
            self,
            index: int,
            series: np.ndarray,
            windows: Iterable[int]) -> ChangePoint:

        (start, end) = self.find_window(index, windows)
        left = series[start:index]
        right = series[index:end]
        mean_l = np.mean(left)
        mean_r = np.mean(right)
        std_l = np.std(left)
        std_r = np.std(right)
        (_, p) = mannwhitneyu(left, right, alternative='two-sided')
        return ChangePoint(index, mean_l, mean_r, std_l, std_r, pvalue=p)


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
            self,
            index: int,
            series: np.ndarray,
            windows: Iterable[int]) -> ChangePoint:

        (start, end) = self.find_window(index, windows)
        left = series[start:index]
        right = series[index:end]

        mean_l = np.mean(left)
        mean_r = np.mean(right)
        std_l = np.std(left)
        std_r = np.std(right)
        (_, p) = ttest_ind_from_stats(mean_l, std_l, len(left),
                                      mean_r, std_r, len(right),
                                      alternative='two-sided')
        return ChangePoint(index, mean_l, mean_r, std_l, std_r, pvalue=p)


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


def compute_change_points(series: np.array,
                          window_len: int = 30,
                          pvalue: float = 0.001) -> List[ChangePoint]:
    """
    Returns the indexes of change-points in a series.

    Internally it uses the EDivisive algorithm from mongodb-signal-processing
    that recursively splits the series in a way to maximize some measure of
    dissimilarity (denoted qhat) between the split parts.
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
    tester = TTestSignificanceTester(pvalue)
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

    windows = [0] + indexes + [len(series)]
    return [tester.change_point(i, series, windows) for i in indexes]


