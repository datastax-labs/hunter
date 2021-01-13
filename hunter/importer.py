from dataclasses import dataclass
from typing import List, Optional

from hunter.analysis import PerformanceLog
from hunter.fallout import Fallout
from hunter.graphite import DataPoint, Graphite, DataSelector
from hunter.util import merge_sorted


@dataclass
class DataImportError(IOError):
    message: str


class FalloutImporter:
    fallout: Fallout
    graphite: Graphite

    def __init__(self, fallout: Fallout, graphite: Graphite):
        self.fallout = fallout
        self.graphite = graphite

    def fetch(self,
              test_name: str,
              user: Optional[str],
              selector: DataSelector) -> PerformanceLog:
        """
        Loads test data from fallout and graphite.
        Converts raw timeseries data into a columnar format,
        where each metric is represented by a list of floats. All metrics
        have aligned indexes - that is values["foo"][3] applies to the
        the same time point as values["bar"][3]. The time points are extracted
        to a separate column.
        """
        test = self.fallout.get_test(test_name, user)
        graphite_result = self.graphite.fetch(test.graphite_prefix(), selector)
        if not graphite_result:
            raise DataImportError(
                f"No timeseries found in Graphite for test {test_name}. "
                "You can define which metrics are fetched from Graphite by "
                "setting the `suffixes` property in the configuration file.")

        times = [[x.time for x in series.points] for series in graphite_result]
        time: List[int] = merge_sorted(times)

        def column(series: List[DataPoint]) -> List[float]:
            value_by_time = dict([(x.time, x.value) for x in series])
            return [value_by_time.get(t) for t in time]

        values = {}
        for ts in graphite_result:
            values[ts.name] = column(ts.points)
        return PerformanceLog(test_name, time, values)
