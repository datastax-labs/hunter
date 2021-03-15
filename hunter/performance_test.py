import logging
from dataclasses import dataclass
from itertools import groupby
from typing import Dict, List, Optional

from numpy import mean

from hunter.analysis import fill_missing, compute_change_points
from hunter.util import sliding_window


@dataclass
class Change:
    metric: str
    index: int
    time: int
    old_mean: float
    new_mean: float
    pvalue: float

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
            for c in change_points:
                changes.append(Change(
                    index=c.index,
                    time=self.time[c.index],
                    metric=metric,
                    old_mean=c.mean_l,
                    new_mean=c.mean_r,
                    pvalue=c.pvalue))

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
