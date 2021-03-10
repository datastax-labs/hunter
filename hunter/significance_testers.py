from typing import Iterable

from more_itertools import pairwise
from signal_processing_algorithms.e_divisive.base import SignificanceTester
from signal_processing_algorithms.e_divisive.change_points import EDivisiveChangePoint

from scipy.stats import mannwhitneyu
from scipy.stats import ttest_ind

import numpy as np


class MannWhitneySignificanceTester(SignificanceTester):
    """
    Uses two-sided Mann-Whitney test to decide if a candidate change point
    splits the series into pieces that are significantly different from each other.
    Does not require data to be normally distributed, but doesn't work well if the
    number of points is smaller than 30.
    """

    pvalue: float

    def __init__(self, pvalue: float):
        self.pvalue = pvalue

    def is_significant(
            self, candidate: EDivisiveChangePoint, series: np.ndarray, windows: Iterable[int]
    ) -> bool:
        (start, end) = next((a, b) for (a, b) in pairwise(windows) if a <= candidate.index < b)
        left = series[start:candidate.index]
        right = series[candidate.index:end]
        (_, p) = mannwhitneyu(left, right, alternative='two-sided')
        return p <= self.pvalue


class TTestSignificanceTester(SignificanceTester):
    """
    Uses two-sided Student's T-test to decide if a candidate change point
    splits the series into pieces that are significantly different from each other.
    This test is good if the data between the change points have normal distribution.
    It works well even with tiny numbers of points (<10).
    """

    pvalue: float

    def __init__(self, pvalue: float):
        self.pvalue = pvalue

    def is_significant(
            self, candidate: EDivisiveChangePoint, series: np.ndarray, windows: Iterable[int]
    ) -> bool:

        (start, end) = next((a, b) for (a, b) in pairwise(windows) if a <= candidate.index < b)
        left = series[start:candidate.index]
        right = series[candidate.index:end]

        if len(left) < 2:
            return False
        if len(right) < 2:
            return False

        (_, p) = ttest_ind(left, right, alternative='two-sided')
        return p <= self.pvalue

