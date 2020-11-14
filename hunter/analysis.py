from dataclasses import dataclass
from typing import List, Dict, Set

from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.calculators import cext_calculator
from signal_processing_algorithms.e_divisive.significance_test import QHatPermutationsSignificanceTester
from tabulate import tabulate

from hunter.util import remove_common_prefix


@dataclass
class ChangePoint:
    time: int
    probability: float
    metrics: List[str]


@dataclass
class TestResults:
    """
    Stores values of interesting metrics of all runs of
    a fallout test indexed by a single time variable.
    Provides utilities to analyze data e.g. find change points.
    """

    test_name: str
    time: List[int]
    values: Dict[str, List[float]]

    def find_change_points(self) -> List[ChangePoint]:
        calculator = cext_calculator
        tester = QHatPermutationsSignificanceTester(
            calculator, pvalue=0.01, permutations=100
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

        return sorted(list(change_points.values()), key=lambda x: x.time)

    def display(self):
        table = {"time": self.time, **self.values}
        headers = ["time", *remove_common_prefix(list(self.values.keys()))]
        print(tabulate(table, headers=headers))



