import csv

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict

from hunter.config import Config
from hunter.csv import CsvColumnType, CsvOptions
from hunter.data_selector import DataSelector
from hunter.fallout import Fallout
from hunter.graphite import DataPoint, Graphite
from hunter.series import Series
from hunter.test_config import CsvTestConfig, FalloutTestConfig, TestConfig
from hunter.util import (
    merge_sorted,
    parse_datetime,
    DateFormatError,
    sliding_window,
    is_float,
    is_datetime,
    remove_prefix,
    resolution,
    round,
)


@dataclass
class DataImportError(IOError):
    message: str


class Importer:
    """
    The Importer interface is responsible for importing performance metric data + metadata from some specified data
    source, and creating an appropriate PerformanceLog object from this imported data.
    """

    def fetch(self, test_conf: TestConfig, selector: DataSelector = DataSelector()) -> Series:
        raise NotImplementedError

    def fetch_all_metric_names(self, test_conf: TestConfig) -> List[str]:
        raise NotImplementedError


class FalloutImporter(Importer):
    fallout: Fallout
    graphite: Graphite

    def __init__(self, fallout: Fallout, graphite: Graphite):
        self.fallout = fallout
        self.graphite = graphite

    def fetch(
        self, test_conf: FalloutTestConfig, selector: DataSelector = DataSelector()
    ) -> Series:
        """
        Loads test data from fallout and graphite.
        Converts raw timeseries data into a columnar format,
        where each metric is represented by a list of floats. All metrics
        have aligned indexes - that is values["foo"][3] applies to the
        the same time point as values["bar"][3]. The time points are extracted
        to a separate column.
        """
        user = test_conf.user if test_conf.user is not None else self.fallout.get_user()
        test_name = test_conf.name
        test = self.fallout.get_test(test_name, user)
        # if no suffixes were specified, use all available ones
        suffixes = (
            test_conf.suffixes
            if test_conf.suffixes is not None
            else self.fetch_all_suffixes(test_conf)
        )

        graphite_result = self.graphite.fetch_data(test.graphite_prefix(), suffixes, selector)
        if not graphite_result:
            raise DataImportError(
                f"No timeseries found in Graphite for test {test_name}. "
                "You can define which metrics are fetched from Graphite by "
                "setting the `suffixes` property in the configuration file."
            )

        times = [[x.time for x in series.points] for series in graphite_result]
        time: List[int] = merge_sorted(times)

        def column(series: List[DataPoint]) -> List[float]:
            value_by_time = dict([(x.time, x.value) for x in series])
            return [value_by_time.get(t) for t in time]

        values = {}
        for ts in graphite_result:
            values[ts.name] = column(ts.points)

        events = self.graphite.fetch_events(
            test_name, user, selector.from_time, selector.until_time
        )

        time_resolution = resolution(time)
        events_by_time = {}
        for e in events:
            events_by_time[round(e.pub_time, time_resolution)] = e

        run_ids = []
        commits = []
        versions = []
        branches = []
        for t in time:
            event = events_by_time.get(t)
            run_ids.append(event.run_id if event is not None else None)
            commits.append(event.commit if event is not None else None)
            versions.append(event.version if event is not None else None)
            branches.append(event.branch if event is not None else None)

        tags = {"run": run_ids, "branch": branches, "version": versions, "commit": commits}
        if selector.attributes is not None:
            tags = {tag: tags[tag] for tag in selector.attributes}
        return Series(test_name, time, values, tags)

    def fetch_all_metric_names(self, test_conf: FalloutTestConfig) -> List[str]:
        test = self.fallout.get_test(test_conf.name, test_conf.user)
        prefix = test.graphite_prefix()
        return self.graphite.fetch_metric_paths(prefix)

    def fetch_all_suffixes(self, test_conf: FalloutTestConfig) -> List[str]:
        metric_paths = self.fetch_all_metric_names(test_conf)
        prefix = self.fallout.get_test(test_conf.name, test_conf.user).graphite_prefix()
        return sorted(
            list(
                set([remove_prefix(path, f"{prefix}.").rpartition(".")[0] for path in metric_paths])
            )
        )


class CsvImporter(Importer):

    __options: CsvOptions

    def __init__(self, options: CsvOptions = CsvOptions()):
        self.__options = options

    def check_row_len(self, headers, row):
        if len(row) < len(headers):
            raise DataImportError(
                "Number of values in the row does not match "
                "number of columns in the table header: " + str(row)
            )

    def check_has_column(self, column: str, headers: List[str]):
        if column not in headers:
            raise DataImportError("Column not found: " + column)

    def column_types(self, file: Path) -> List[CsvColumnType]:
        """
        Guesses data types based on values in the table.
        If all values in the column can be converted to a float, then Numeric type is assumed.
        If some values cannot be converted to a float, then an attempt is made to parse them
        as datetime objects. If some of the values are neither float or datetime, then the
        column is assumed to contain strings.
        """
        with open(file, newline="") as csv_file:
            reader = csv.reader(
                csv_file, delimiter=self.__options.delimiter, quotechar=self.__options.quote_char
            )
            headers: List[str] = next(reader, None)
            types = [CsvColumnType.Numeric] * len(headers)

            for row in reader:
                self.check_row_len(headers, row)
                for i in range(len(types)):
                    if types[i] == CsvColumnType.Numeric and not is_float(row[i]):
                        types[i] = CsvColumnType.DateTime
                    if types[i] == CsvColumnType.DateTime and not is_datetime(row[i]):
                        types[i] = CsvColumnType.Str
            return types

    def time_column_index(self, headers: List[str], types: List[CsvColumnType]) -> int:
        """
        Returns the index of the time column. If time column name is given in the CsvOptions,
        then it is looked up. Otherwise the first column with DateTime type will be used.
        """
        if self.__options.time_column is None:
            datetime_indexes = (i for i, t in enumerate(types) if t == CsvColumnType.DateTime)
            time_index = next(datetime_indexes, None)
            if time_index is None:
                raise DataImportError("No time column found")
            return time_index
        else:
            time_column = self.__options.time_column
            self.check_has_column(time_column, headers)
            return headers.index(time_column)

    def attr_indexes(
        self, attributes: Optional[List[str]], headers: List[str], types: List[CsvColumnType]
    ) -> List[int]:
        if attributes is None:
            return [
                i
                for i, t in enumerate(types)
                if t == CsvColumnType.Str or t == CsvColumnType.DateTime
            ]
        else:
            for c in attributes:
                self.check_has_column(c, headers)
            return [headers.index(c) for c in attributes]

    def metric_indexes(
        self, metrics: Optional[List[str]], headers: List[str], types: List[CsvColumnType]
    ) -> List[int]:
        if metrics is None:
            return [i for i, t in enumerate(types) if t == CsvColumnType.Numeric]
        else:
            for c in metrics:
                self.check_has_column(c, headers)
            return [headers.index(c) for c in metrics]

    def fetch(self, test_conf: CsvTestConfig, selector: DataSelector = DataSelector()) -> Series:
        file = Path(test_conf.name)
        from_time = selector.from_time
        until_time = selector.until_time

        with open(file, newline="") as csv_file:
            reader = csv.reader(
                csv_file, delimiter=self.__options.delimiter, quotechar=self.__options.quote_char
            )

            headers: List[str] = next(reader, None)
            types: List[CsvColumnType] = self.column_types(file)

            # Decide which columns to fetch into which components of the result:
            time_index: int = self.time_column_index(headers, types)
            attr_indexes: List[int] = self.attr_indexes(selector.attributes, headers, types)
            metric_indexes: List[int] = self.metric_indexes(selector.metrics, headers, types)
            if time_index in attr_indexes:
                attr_indexes.remove(time_index)
            if time_index in metric_indexes:
                metric_indexes.remove(time_index)

            # Initialize empty lists to store the data and metadata:
            time: List[int] = []
            data: Dict[str, List[float]] = {}
            for i in metric_indexes:
                data[headers[i]] = []
            attributes: Dict[str, List[str]] = {}
            for i in attr_indexes:
                attributes[headers[i]] = []

            # Append the lists with data from each row:
            for row in reader:
                self.check_row_len(headers, row)

                # Filter by time:
                ts: datetime = self.__convert_time(row[time_index])
                if from_time is not None and ts < from_time:
                    continue
                if until_time is not None and ts >= until_time:
                    continue
                time.append(int(ts.timestamp()))

                # Read metric values. Note we can still fail on conversion to float,
                # because the user is free to override the column selection and thus
                # they may select a column that contains non-numeric data:
                for i in metric_indexes:
                    try:
                        data[headers[i]].append(float(row[i]))
                    except ValueError as err:
                        raise DataImportError(
                            "Could not convert value in column " + headers[i] + ": " + err.args[0]
                        )

                # Attributes are just copied as-is, with no conversion:
                for i in attr_indexes:
                    attributes[headers[i]].append(row[i])

            return Series(str(file.name), time, data, attributes)

    def fetch_all_metric_names(self, test_conf: CsvTestConfig) -> List[str]:
        metrics = []
        file = Path(test_conf.name)
        with open(file, newline="") as csv_file:
            reader = csv.reader(
                csv_file, delimiter=self.__options.delimiter, quotechar=self.__options.quote_char
            )

            headers: List[str] = next(reader, None)
            types: List[CsvColumnType] = self.column_types(file)
            metric_indexes = self.metric_indexes(metrics=None, headers=headers, types=types)
            for metric_index in metric_indexes:
                metrics.append(headers[metric_index])
        return metrics

    def __select_columns(self, headers, selector):
        value_indexes = [
            i
            for i in range(len(headers))
            if headers[i] != self.__options.time_column
            and (selector.attributes is None or headers[i] not in selector.attributes)
            and (selector.metrics is None or headers[i] in selector.metrics)
        ]
        if len(value_indexes) == 0:
            raise DataImportError("No metrics found")
        return value_indexes

    def __convert_time(self, time: str):
        try:
            return parse_datetime(time)
        except DateFormatError as err:
            raise DataImportError(err.message)


def get_importer(test_conf: TestConfig, conf: Config) -> Importer:
    if isinstance(test_conf, CsvTestConfig):
        return CsvImporter(options=test_conf.csv_options)
    if isinstance(test_conf, FalloutTestConfig):
        return FalloutImporter(fallout=Fallout(conf.fallout), graphite=Graphite(conf.graphite))
