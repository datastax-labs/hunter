from typing import List, Optional

from hunter.analysis import TestResults
from hunter.fallout import Fallout
from hunter.graphite import DataPoint, Graphite
from hunter.util import merge_sorted


class FalloutImporter:
    fallout: Fallout
    graphite: Graphite

    def __init__(self, fallout: Fallout, graphite: Graphite):
        self.fallout = fallout
        self.graphite = graphite

    def fetch(self, test_name: str, user: Optional[str] = None) -> TestResults:
        """
        Loads test data from fallout and graphite.
        Converts raw timeseries data into a columnar format,
        where each metric is represented by a list of floats. All metrics
        have aligned indexes - that is values["foo"][3] applies to the
        the same time point as values["bar"][3]. The time points are extracted
        to a separate column.
        """
        test = self.fallout.get_test(test_name, user)
        data = self.graphite.fetch(test.graphite_prefix())
        assert data, "no timeseries found"

        times = [[x.time for x in series.data] for series in data]
        time: List[int] = merge_sorted(times)

        def column(series: List[DataPoint]) -> List[float]:
            value_by_time = dict([(x.time, x.value) for x in series])
            return [value_by_time.get(t) for t in time]

        values = {}
        for ts in data:
            values[ts.name] = column(ts.data)
        return TestResults(test_name, time, values)
