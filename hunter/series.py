import logging
from dataclasses import dataclass
from itertools import groupby
from typing import Dict, List, Optional

from hunter.analysis import (
    fill_missing,
    compute_change_points,
    ComparativeStats,
    TTestSignificanceTester,
)

import numpy as np


@dataclass
class AnalysisOptions:
    window_len: int
    max_pvalue: float
    min_magnitude: float

    def __init__(self):
        self.window_len = 50
        self.max_pvalue = 0.001
        self.min_magnitude = 0.0


@dataclass
class ChangePoint:
    """A change-point for a single metric"""

    metric: str
    index: int
    time: int
    stats: ComparativeStats

    def forward_change_percent(self) -> float:
        return self.stats.forward_rel_change() * 100.0

    def backward_change_percent(self) -> float:
        return self.stats.backward_rel_change() * 100.0

    def magnitude(self):
        return self.stats.change_magnitude()


@dataclass
class ChangePointGroup:
    """A group of change points on multiple metrics, at the same time"""

    index: int
    time: int
    prev_time: int
    attributes: Dict[str, str]
    prev_attributes: Dict[str, str]
    changes: List[ChangePoint]


class Series:
    """
    Stores values of interesting metrics of all runs of
    a fallout test indexed by a single time variable.
    Provides utilities to analyze data e.g. find change points.
    """

    test_name: str
    time: List[int]
    attributes: Dict[str, List[str]]
    data: Dict[str, List[float]]

    def __init__(
        self,
        test_name: str,
        time: List[int],
        data: Dict[str, List[float]],
        metadata: Dict[str, List[str]],
    ):
        self.test_name = test_name
        self.time = time
        self.attributes = metadata
        self.data = data
        assert all(len(x) == len(time) for x in data.values())
        assert all(len(x) == len(time) for x in metadata.values())

    def attributes_at(self, index: int) -> Dict[str, str]:
        result = {}
        for (k, v) in self.attributes.items():
            result[k] = v[index]
        return result

    def change_points(
        self, metric: str, options: AnalysisOptions = AnalysisOptions()
    ) -> List[ChangePoint]:

        values = self.data[metric].copy()
        fill_missing(values)
        change_points = compute_change_points(
            values,
            window_len=options.window_len,
            max_pvalue=options.max_pvalue,
            min_magnitude=options.min_magnitude,
        )
        result = []
        for c in change_points:
            result.append(
                ChangePoint(index=c.index, time=self.time[c.index], metric=metric, stats=c.stats)
            )
        return result

    def all_change_points(
        self, options: AnalysisOptions = AnalysisOptions()
    ) -> List[ChangePointGroup]:

        if len(self.time) == 0:
            return []

        logging.info(f"Computing change points for test {self.test_name}...")
        changes: List[ChangePoint] = []
        for metric in self.data.keys():
            changes += self.change_points(metric, options)

        changes.sort(key=lambda c: c.index)
        points = []
        for k, g in groupby(changes, key=lambda c: c.index):
            cp = ChangePointGroup(
                index=k,
                time=self.time[k],
                prev_time=self.time[k - 1],
                attributes=self.attributes_at(k),
                prev_attributes=self.attributes_at(k - 1),
                changes=list(g),
            )
            points.append(cp)

        return points

    def get_stable_range(self, index: int, change_points: List[ChangePoint]) -> (int, int):
        """
        Returns a range of indexes (A, B) such that:
          - A is the nearest change point index of the `metric` before or equal given `index`,
            or 0 if not found
          - B is the nearest change point index of the `metric` after given `index,
            or len(self.time) if not found

        It follows that there are no change points between A and B.
        """
        begin = 0
        for cp in change_points:
            if cp.index > index:
                break
            begin = cp.index

        end = len(self.time)
        for cp in reversed(change_points):
            if cp.index <= index:
                break
            end = cp.index

        return begin, end


@dataclass
class SeriesComparison:
    series_1: Series
    series_2: Series
    index_1: int
    index_2: int
    stats: Dict[str, ComparativeStats]  # keys: metric name


def compare(
    series_1: Series,
    index_1: Optional[int],
    series_2: Series,
    index_2: Optional[int],
    options: AnalysisOptions = AnalysisOptions(),
) -> SeriesComparison:

    # if index not specified, we want to take the most recent performance
    index_1 = index_1 if index_1 is not None else len(series_1.time)
    index_2 = index_2 if index_2 is not None else len(series_2.time)
    metrics = set(series_1.data.keys()).intersection(series_2.data.keys())

    tester = TTestSignificanceTester(options.max_pvalue)
    stats = {}

    for metric in metrics:
        change_points_1 = series_1.change_points(metric, options)
        (begin_1, end_1) = series_1.get_stable_range(index_1, change_points_1)
        data_1 = series_1.data[metric][begin_1:end_1]

        change_points_2 = series_2.change_points(metric, options)
        (begin_2, end_2) = series_2.get_stable_range(index_2, change_points_2)
        data_2 = series_2.data[metric][begin_2:end_2]

        stats[metric] = tester.compare(np.array(data_1), np.array(data_2))

    return SeriesComparison(series_1, series_2, index_1, index_2, stats)
