import datetime
from dataclasses import dataclass
from typing import List, Dict, Set, Optional

from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.calculators import cext_calculator
from signal_processing_algorithms.e_divisive.significance_test import QHatPermutationsSignificanceTester
from tabulate import tabulate

from hunter.util import remove_common_prefix, format_timestamp


@dataclass
class ChangePoint:
    time: int
    probability: float
    metrics: List[str]


class TestResults:
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

        calculator = cext_calculator
        tester = QHatPermutationsSignificanceTester(
            calculator, pvalue=0.05, permutations=100
        )
        algo = EDivisive(seed=None, calculator=calculator, significance_tester=tester)

        change_points = {}
        for metric, values in self.values.items():
            for cp in algo.get_change_points(values):
                if cp.index in change_points:
                    c = change_points[cp.index]
                    c.metrics.append(metric)
                    c.probability = min(c.probability, cp.probability)
                else:
                    change_points[cp.index] = ChangePoint(
                        self.time[cp.index],
                        cp.probability,
                        [metric]
                    )
        self.change_points = \
            sorted(list(change_points.values()), key=lambda x: x.time)
        return self.change_points

    def format_log(self) -> str:
        time_column = [format_timestamp(ts) for ts in self.time]
        table = {"time": time_column, **self.values}
        headers = ["time", *remove_common_prefix(list(self.values.keys()))]
        return tabulate(table, headers=headers)

    def format_change_points(self):
        change_points = [
            [format_timestamp(cp.time),
             cp.probability,
             remove_common_prefix(cp.metrics)]
            for cp in self.find_change_points()]
        return tabulate(change_points, ["time", "P-value", "metrics"])


