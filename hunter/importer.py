import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict

from hunter.analysis import PerformanceLog
from hunter.fallout import Fallout
from hunter.graphite import DataPoint, Graphite, DataSelector
from hunter.util import merge_sorted, parse_datetime, DateFormatError


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
              selector: DataSelector = DataSelector()) -> PerformanceLog:
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



@dataclass
class CsvOptions:
    delimiter: str
    quote_char: str
    time_column: str

    def __init__(self):
        self.delimiter = ','
        self.quote_char = '"'
        self.time_column = "time"

class CsvImporter:

    __options: CsvOptions

    def __init__(self, options: CsvOptions = CsvOptions()):
        self.__options = options


    def fetch(self,
              file: Path,
              selector: DataSelector = DataSelector()) -> PerformanceLog:
        from_time = selector.from_time
        until_time = selector.until_time
        time_column = self.__options.time_column

        with open(file, newline='') as csv_file:
            reader = csv.reader(csv_file,
                                delimiter=self.__options.delimiter,
                                quotechar=self.__options.quote_char)

            headers: List[str] = next(reader, None)
            if time_column not in headers:
                raise DataImportError("Column not found: " + time_column)
            time_column_index = headers.index(time_column)
            metric_indexes = self.__select_columns(headers, selector)

            time: List[int] = []
            values: Dict[str, List[float]] = {}
            for i in metric_indexes:
                values[headers[i]] = []

            for row in reader:
                ts = self.__convert_time(row[time_column_index])
                if from_time is not None and ts < from_time:
                    continue
                if until_time is not None and ts >= until_time:
                    continue
                time.append(int(ts.timestamp()))
                for i in metric_indexes:
                    values[headers[i]].append(float(row[i]))

            if len(time) == 0:
                raise DataImportError("No matching data rows found")

            return PerformanceLog(str(file.name), time, values)

    def __select_columns(self, headers, selector):
        value_indexes = \
            [i
             for i in range(len(headers))
             if headers[i] != self.__options.time_column
             and (selector.metrics is None
                  or headers[i] in selector.metrics)]
        if len(value_indexes) == 0:
            raise DataImportError("No metrics found")
        return value_indexes

    def __convert_time(self, time: str):
        try:
            return parse_datetime(time)
        except DateFormatError as err:
            raise DataImportError(err.message)

