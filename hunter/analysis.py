from dataclasses import dataclass
from typing import List, Dict, Optional

from signal_processing_algorithms.e_divisive import EDivisive
from signal_processing_algorithms.e_divisive.calculators import cext_calculator
from signal_processing_algorithms.e_divisive.significance_test import \
    QHatPermutationsSignificanceTester


@dataclass
class ChangePoint:
    index: int
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
            calculator, pvalue=0.01, permutations=100
        )
        change_points = {}
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
            for cp in algo.get_change_points(values):
                if cp.index in change_points:
                    c = change_points[cp.index]
                    c.metrics.append(metric)
                    c.probability = min(c.probability, cp.probability)
                else:
                    change_points[cp.index] = ChangePoint(
                        cp.index,
                        self.time[cp.index],
                        cp.probability,
                        [metric]
                    )
        self.change_points = \
            sorted(list(change_points.values()), key=lambda x: x.time)
        return self.change_points



