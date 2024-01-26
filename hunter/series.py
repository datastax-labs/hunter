import logging
from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
from typing import Dict, Iterable, List, Optional

import numpy as np

from hunter.analysis import (
    ComparativeStats,
    TTestSignificanceTester,
    compute_change_points,
    compute_change_points_orig,
    fill_missing,
)


@dataclass
class AnalysisOptions:
    window_len: int
    max_pvalue: float
    min_magnitude: float
    orig_edivisive: bool

    def __init__(self):
        self.window_len = 50
        self.max_pvalue = 0.001
        self.min_magnitude = 0.0
        self.orig_edivisive = False


@dataclass
class Metric:
    direction: int
    scale: float
    unit: str

    def __init__(self, direction: int = 1, scale: float = 1.0, unit: str = ""):
        self.direction = direction
        self.scale = scale
        self.unit = ""


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

    def to_json(self):
        return {
            "metric": self.metric,
            "forward_change_percent": f"{self.forward_change_percent():.0f}",
        }


@dataclass
class ChangePointGroup:
    """A group of change points on multiple metrics, at the same time"""

    index: int
    time: float
    prev_time: int
    attributes: Dict[str, str]
    prev_attributes: Dict[str, str]
    changes: List[ChangePoint]

    def to_json(self):
        return {"time": self.time, "changes": [cp.to_json() for cp in self.changes]}


class Series:
    """
    Stores values of interesting metrics of all runs of
    a fallout test indexed by a single time variable.
    Provides utilities to analyze data e.g. find change points.
    """

    test_name: str
    branch: Optional[str]
    time: List[int]
    metrics: Dict[str, Metric]
    attributes: Dict[str, List[str]]
    data: Dict[str, List[float]]

    def __init__(
        self,
        test_name: str,
        branch: Optional[str],
        time: List[int],
        metrics: Dict[str, Metric],
        data: Dict[str, List[float]],
        attributes: Dict[str, List[str]],
    ):
        self.test_name = test_name
        self.branch = branch
        self.time = time
        self.metrics = metrics
        self.attributes = attributes if attributes else {}
        self.data = data
        assert all(len(x) == len(time) for x in data.values())
        assert all(len(x) == len(time) for x in attributes.values())

    def attributes_at(self, index: int) -> Dict[str, str]:
        result = {}
        for (k, v) in self.attributes.items():
            result[k] = v[index]
        return result

    def find_first_not_earlier_than(self, time: datetime) -> Optional[int]:
        timestamp = time.timestamp()
        for i, t in enumerate(self.time):
            if t >= timestamp:
                return i
        return None

    def find_by_attribute(self, name: str, value: str) -> List[int]:
        """Returns the indexes of data points with given attribute value"""
        result = []
        for i in range(len(self.time)):
            if self.attributes_at(i).get(name) == value:
                result.append(i)
        return result

    def analyze(self, options: AnalysisOptions = AnalysisOptions()) -> "AnalyzedSeries":
        logging.info(f"Computing change points for test {self.test_name}...")
        return AnalyzedSeries(self, options)


class AnalyzedSeries:
    """
    Time series data with computed change points.
    """

    __series: Series
    options: AnalysisOptions
    change_points: Dict[str, List[ChangePoint]]
    change_points_by_time: List[ChangePointGroup]

    def __init__(self, series: Series, options: AnalysisOptions):
        self.__series = series
        self.options = options
        self.change_points = self.__compute_change_points(series, options)
        self.change_points_by_time = self.__group_change_points_by_time(series, self.change_points)

    @staticmethod
    def __compute_change_points(
        series: Series, options: AnalysisOptions
    ) -> Dict[str, List[ChangePoint]]:
        result = {}
        for metric in series.data.keys():
            values = series.data[metric].copy()
            fill_missing(values)
            if options.orig_edivisive:
                change_points = compute_change_points_orig(
                    values,
                    max_pvalue=options.max_pvalue,
                )
            else:
                change_points = compute_change_points(
                    values,
                    window_len=options.window_len,
                    max_pvalue=options.max_pvalue,
                    min_magnitude=options.min_magnitude,
                )
            result[metric] = []
            for c in change_points:
                result[metric].append(
                    ChangePoint(
                        index=c.index, time=series.time[c.index], metric=metric, stats=c.stats
                    )
                )
        return result

    @staticmethod
    def __group_change_points_by_time(
        series: Series, change_points: Dict[str, List[ChangePoint]]
    ) -> List[ChangePointGroup]:
        changes: List[ChangePoint] = []
        for metric in change_points.keys():
            changes += change_points[metric]

        changes.sort(key=lambda c: c.index)
        points = []
        for k, g in groupby(changes, key=lambda c: c.index):
            cp = ChangePointGroup(
                index=k,
                time=series.time[k],
                prev_time=series.time[k - 1],
                attributes=series.attributes_at(k),
                prev_attributes=series.attributes_at(k - 1),
                changes=list(g),
            )
            points.append(cp)

        return points

    def get_stable_range(self, metric: str, index: int) -> (int, int):
        """
        Returns a range of indexes (A, B) such that:
          - A is the nearest change point index of the `metric` before or equal given `index`,
            or 0 if not found
          - B is the nearest change point index of the `metric` after given `index,
            or len(self.time) if not found

        It follows that there are no change points between A and B.
        """
        begin = 0
        for cp in self.change_points[metric]:
            if cp.index > index:
                break
            begin = cp.index

        end = len(self.time())
        for cp in reversed(self.change_points[metric]):
            if cp.index <= index:
                break
            end = cp.index

        return begin, end

    def test_name(self) -> str:
        return self.__series.test_name

    def branch_name(self) -> Optional[str]:
        return self.__series.branch

    def len(self) -> int:
        return len(self.__series.time)

    def time(self) -> List[int]:
        return self.__series.time

    def data(self, metric: str) -> List[float]:
        return self.__series.data[metric]

    def attributes(self) -> Iterable[str]:
        return self.__series.attributes.keys()

    def attributes_at(self, index: int) -> Dict[str, str]:
        return self.__series.attributes_at(index)

    def attribute_values(self, attribute: str) -> List[str]:
        return self.__series.attributes[attribute]

    def metric_names(self) -> Iterable[str]:
        return self.__series.metrics.keys()

    def metric(self, name: str) -> Metric:
        return self.__series.metrics[name]


@dataclass
class SeriesComparison:
    series_1: AnalyzedSeries
    series_2: AnalyzedSeries
    index_1: int
    index_2: int
    stats: Dict[str, ComparativeStats]  # keys: metric name


def compare(
    series_1: AnalyzedSeries,
    index_1: Optional[int],
    series_2: AnalyzedSeries,
    index_2: Optional[int],
) -> SeriesComparison:

    # if index not specified, we want to take the most recent performance
    index_1 = index_1 if index_1 is not None else len(series_1.time())
    index_2 = index_2 if index_2 is not None else len(series_2.time())
    metrics = filter(lambda m: m in series_2.metric_names(), series_1.metric_names())

    tester = TTestSignificanceTester(series_1.options.max_pvalue)
    stats = {}

    for metric in metrics:
        data_1 = series_1.data(metric)
        (begin_1, end_1) = series_1.get_stable_range(metric, index_1)
        data_1 = [x for x in data_1[begin_1:end_1] if x is not None]

        data_2 = series_2.data(metric)
        (begin_2, end_2) = series_2.get_stable_range(metric, index_2)
        data_2 = [x for x in data_2[begin_2:end_2] if x is not None]

        stats[metric] = tester.compare(np.array(data_1), np.array(data_2))

    return SeriesComparison(series_1, series_2, index_1, index_2, stats)
