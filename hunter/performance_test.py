import logging
from dataclasses import dataclass
from itertools import groupby
from typing import Dict, List

from hunter.analysis import fill_missing, compute_change_points, ComparativeStats


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
class Change:
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
class ChangePoint:
    """A group of change points on multiple metrics, at the same time"""

    index: int
    time: int
    prev_time: int
    attributes: Dict[str, str]
    prev_attributes: Dict[str, str]
    changes: List[Change]


class PerformanceTest:
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
        self.change_points = None
        assert all(len(x) == len(time) for x in data.values())
        assert all(len(x) == len(time) for x in metadata.values())

    def attributes_at(self, index: int) -> Dict[str, str]:
        result = {}
        for (k, v) in self.attributes.items():
            result[k] = v[index]
        return result

    def find_change_points(
        self, analysis_conf: AnalysisOptions = AnalysisOptions()
    ) -> List[ChangePoint]:
        if self.change_points is not None:
            return self.change_points
        if len(self.time) == 0:
            return []

        logging.info("Computing change points...")
        changes: List[Change] = []
        for metric, values in self.data.items():
            values = values.copy()
            fill_missing(values)
            change_points = compute_change_points(
                values,
                window_len=analysis_conf.window_len,
                max_pvalue=analysis_conf.max_pvalue,
                min_magnitude=analysis_conf.min_magnitude,
            )
            for c in change_points:
                changes.append(
                    Change(index=c.index, time=self.time[c.index], metric=metric, stats=c.stats)
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
                changes=list(g),
            )
            points.append(cp)

        self.change_points = points
        return points
