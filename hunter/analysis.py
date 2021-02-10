import logging
from dataclasses import dataclass
from itertools import groupby
from statistics import mean
from typing import List, Dict, Optional

from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.calculators import cext_calculator
from signal_processing_algorithms.e_divisive.change_points import \
    EDivisiveChangePoint
from signal_processing_algorithms.e_divisive.significance_test import \
    QHatPermutationsSignificanceTester

from hunter.util import sliding_window


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


@dataclass
class Change:
    metric: str
    index: int
    time: int
    probability: float
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
        calculator = cext_calculator
        tester = QHatPermutationsSignificanceTester(
            calculator, pvalue=0.05, permutations=100
        )
        changes: List[Change] = []
        for metric, values in self.data.items():
            # We need to initialize a fresh algo instance for each metric
            # because calling get_change_points
            # on the same instance modifies the internal state and
            # yields weird results than when called
            # separately. But we want to find change points separately for
            # each metric here and take a simple sum of them.
            algo = EDivisive(seed=None,
                             calculator=calculator,
                             significance_tester=tester)

            values = values.copy()
            fill_missing(values)
            init = EDivisiveChangePoint(0)
            end = EDivisiveChangePoint(len(values))
            change_points = algo.get_change_points(values)
            change_points.sort(key=lambda c: c.index)

            for window in sliding_window([init, *change_points, end], 3):
                prev_cp = window[0]
                curr_cp = window[1]
                next_cp = window[2]
                old_mean = mean(filter(None.__ne__,
                                       values[prev_cp.index:curr_cp.index]))
                new_mean = mean(filter(None.__ne__,
                                       values[curr_cp.index:next_cp.index]))
                changes.append(
                    Change(
                        index=curr_cp.index,
                        time=self.time[curr_cp.index],
                        probability=curr_cp.probability,
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
