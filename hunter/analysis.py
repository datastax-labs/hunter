import logging
from dataclasses import dataclass
from itertools import groupby
from statistics import mean
from typing import List, Dict, Optional

from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.calculators import numpy_calculator, cext_calculator
from signal_processing_algorithms.e_divisive.significance_test import \
    QHatPermutationsSignificanceTester

from hunter.util import sliding_window

import numpy as np


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


def compute_change_points(series: np.array, window_len: int = 30, pvalue: float = 0.05) \
        -> List[int]:
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
    while start < len(series):
        end = min(start + window_len, len(series))
        calculator = cext_calculator
        tester = QHatPermutationsSignificanceTester(calculator, pvalue, permutations=100)
        algo = EDivisive(seed=None, calculator=calculator, significance_tester=tester)
        pts = algo.get_change_points(series[start:end])
        new_indexes = [p.index + start for p in pts]
        new_indexes.sort()
        last_new_change_point_index = next(iter(new_indexes[-1:]), 0)
        start = max(last_new_change_point_index, start + step)
        indexes += new_indexes

    return indexes

@dataclass
class Change:
    metric: str
    index: int
    time: int
    old_mean: float
    new_mean: float

    def change_percent(self) -> float:
        return (self.new_mean / self.old_mean - 1.0) * 100.0


@dataclass
class ChangePoint:
    index: int
    time: int
    prev_time: int
    attributes: Dict[str, str]
    prev_attributes: Dict[str, str]
    changes: List[Change]


class PerformanceLog:
    """
    Stores values of interesting metrics of all runs of
    a fallout test indexed by a single time variable.
    Provides utilities to analyze data e.g. find change points.
    """

    test_name: str
    time: List[int]
    attributes: Dict[str, List[str]]
    data: Dict[str, List[float]]
    change_points: Optional[List[ChangePoint]]

    def __init__(self,
                 test_name: str,
                 time: List[int],
                 data: Dict[str, List[float]],
                 metadata: Dict[str, List[str]]):
        self.test_name = test_name
        self.time = time
        self.attributes = metadata
        self.data = data
        self.change_points = None
        assert all(len(x) == len(time) for x in data.values())
        assert all(len(x) == len(time) for x in metadata.values())

    def attributes_at(self, index: int) -> Dict[str, str]:
        result = {}
        for (k, v) in self.attributes.items():
            result[k] = v[index]
        return result

    def find_change_points(self) -> List[ChangePoint]:
        if self.change_points is not None:
            return self.change_points
        if len(self.time) == 0:
            return []

        logging.info("Computing change points...")
        changes: List[Change] = []
        for metric, values in self.data.items():
            values = values.copy()
            fill_missing(values)
            change_points = compute_change_points(values)
            change_points.sort()
            for window in sliding_window([0, *change_points, len(values)], 3):
                prev_cp = window[0]
                curr_cp = window[1]
                next_cp = window[2]
                old_mean = mean(filter(None.__ne__,
                                       values[prev_cp:curr_cp]))
                new_mean = mean(filter(None.__ne__,
                                       values[curr_cp:next_cp]))
                changes.append(
                    Change(
                        index=curr_cp,
                        time=self.time[curr_cp],
                        metric=metric,
                        old_mean=old_mean,
                        new_mean=new_mean
                    )
                )

        changes.sort(key=lambda c: c.index)
        points = []
        for k, g in groupby(changes, key=lambda c: c.index):
            cp = ChangePoint(
                index=k,
                time=self.time[k],
                prev_time=self.time[k - 1],
                attributes=self.attributes_at(k),
                prev_attributes=self.attributes_at(k - 1),
                changes=list(g))
            points.append(cp)

        self.change_points = points
        return points
