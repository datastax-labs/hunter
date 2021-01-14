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
    changes: List[Change]


class PerformanceLog:
    """
    Stores values of interesting metrics of all runs of
    a fallout test indexed by a single time variable.
    Provides utilities to analyze data e.g. find change points.
    """

    test_name: str
    time: List[int]
    values: Dict[str, List[float]]
    change_points: Optional[List[ChangePoint]]

    def __init__(self,
                 test_name: str,
                 time: List[int],
                 values: Dict[str, List[float]]):
        self.test_name = test_name
        self.time = time
        self.values = values
        self.change_points = None

    def find_change_points(self) -> List[ChangePoint]:
        if self.change_points is not None:
            return self.change_points

        logging.info("Computing change points...")
        calculator = cext_calculator
        tester = QHatPermutationsSignificanceTester(
            calculator, pvalue=0.05, permutations=100
        )
        changes: List[Change] = []
        for metric, values in self.values.items():
            # We need to initialize a fresh algo instance for each metric
            # because calling get_change_points
            # on the same instance modifies the internal state and
            # yields weird results than when called
            # separately. But we want to find change points separately for
            # each metric here and take a simple sum of them.
            algo = EDivisive(seed=None,
                             calculator=calculator,
                             significance_tester=tester)

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
            cp = ChangePoint(index=k, time=self.time[k], changes=list(g))
            points.append(cp)

        self.change_points = points
        return points
