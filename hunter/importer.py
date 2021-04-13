import csv
from collections import OrderedDict

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

from hunter.config import Config
from hunter.csv_options import CsvColumnType, CsvOptions
from hunter.data_selector import DataSelector
from hunter.graphite import DataPoint, Graphite, GraphiteError
from hunter.series import Series
from hunter.test_config import CsvTestConfig, TestConfig, GraphiteTestConfig
from hunter.util import (
    merge_sorted,
    parse_datetime,
    DateFormatError,
    format_timestamp,
    resolution,
    round,
    is_float,
    is_datetime,
)


@dataclass
class DataImportError(IOError):
    message: str


class Importer:
    """
    An Importer is responsible for importing performance metric data + metadata
    from some specified data source, and creating an appropriate PerformanceLog object
    from this imported data.
    """

    def fetch_data(self, test: TestConfig, selector: DataSelector = DataSelector()) -> Series:
        raise NotImplementedError

    def fetch_all_metric_names(self, test: TestConfig) -> List[str]:
        raise NotImplementedError


class GraphiteImporter(Importer):
    graphite: Graphite

    def __init__(self, graphite: Graphite):
        self.graphite = graphite

    def fetch_data(self, test: TestConfig, selector: DataSelector = DataSelector()) -> Series:
        """
        Loads test data from graphite.
        Converts raw timeseries data into a columnar format,
        where each metric is represented by a list of floats. All metrics
        have aligned indexes - that is values["foo"][3] applies to the
        the same time point as values["bar"][3]. The time points are extracted
        to a separate column.
        """
        if not isinstance(test, GraphiteTestConfig):
            raise ValueError("Expected GraphiteTestConfig")

        try:

            # if the user has specified since_<commit/version> and/or until_<commit/version>,
            # we need to attempt to extract a timestamp from appropriate Graphite events, and
            # update selector.since_time and selector.until_time, respectively
            since_events = self.graphite.fetch_events_with_matching_time_option(
                test.tags, selector.since_commit, selector.since_version
            )
            if len(since_events) > 0:
                # since timestamps of metrics get rounded down, in order to include these, we need to
                # - round down the event's pub_time
                # - subtract a small amount of time (Graphite does not appear to include the left-hand
                # endpoint for a time range)
                rounded_time = round(
                    int(since_events[-1].pub_time.timestamp()),
                    resolution([int(since_events[-1].pub_time.timestamp())]),
                )
                selector.since_time = parse_datetime(str(rounded_time)) - timedelta(milliseconds=1)

            until_events = self.graphite.fetch_events_with_matching_time_option(
                test.tags, selector.until_commit, selector.until_version
            )
            if len(until_events) > 0:
                selector.until_time = until_events[0].pub_time

            if selector.since_time.timestamp() > selector.until_time.timestamp():
                raise DataImportError(
                    f"Invalid time range: ["
                    f"{format_timestamp(int(selector.since_time.timestamp()))}, "
                    f"{format_timestamp(int(selector.until_time.timestamp()))}]"
                )

            metrics = test.metrics.values()
            if selector.metrics is not None:
                metrics = [m for m in test.metrics.values() if m.name in selector.metrics]
            path_to_metric = {test.prefix + "." + m.suffix: m for m in metrics}
            targets = [test.prefix + "." + m.suffix for m in metrics]

            graphite_result = self.graphite.fetch_data(targets, selector)
            if not graphite_result:
                raise DataImportError(f"No timeseries found in Graphite for test {test.name}.")

            times = [[x.time for x in series.points] for series in graphite_result]
            time: List[int] = merge_sorted(times)

            def column(series: List[DataPoint]) -> List[float]:
                value_by_time = dict([(x.time, x.value) for x in series])
                return [value_by_time.get(t) for t in time]

            # Keep order of the keys in the result values the same as order of metrics
            values = OrderedDict()
            for m in metrics:
                values[m.name] = []
            for ts in graphite_result:
                values[path_to_metric[ts.path].name] = column(ts.points)

            events = self.graphite.fetch_events(test.tags, selector.since_time, selector.until_time)
            time_resolution = resolution(time)
            events_by_time = {}
            for e in events:
                events_by_time[round(int(e.pub_time.timestamp()), time_resolution)] = e

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
            return Series(test.name, time, values, tags)

        except GraphiteError as e:
            raise DataImportError(f"Failed to import test {test.name}: {e.message}")

    def fetch_all_metric_names(self, test_conf: GraphiteTestConfig) -> List[str]:
        return [m for m in test_conf.metrics.keys()]


class CsvImporter(Importer):
    @staticmethod
    def check_row_len(headers, row):
        if len(row) < len(headers):
            raise DataImportError(
                "Number of values in the row does not match "
                "number of columns in the table header: " + str(row)
            )

    @staticmethod
    def check_has_column(column: str, headers: List[str]):
        if column not in headers:
            raise DataImportError("Column not found: " + column)

    def __column_types(self, file: Path, csv_options: CsvOptions) -> List[CsvColumnType]:
        """
        Guesses data types based on values in the table.
        If all values in the column can be converted to a float, then Numeric type is assumed.
        If some values cannot be converted to a float, then an attempt is made to parse them
        as datetime objects. If some of the values are neither float or datetime, then the
        column is assumed to contain strings.
        """
        with open(file, newline="") as csv_file:
            reader = csv.reader(
                csv_file, delimiter=csv_options.delimiter, quotechar=csv_options.quote_char
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

    def __time_column_index(
        self, time_column: str, headers: List[str], types: List[CsvColumnType]
    ) -> int:
        """
        Returns the index of the time column. If time column name is given in the CsvOptions,
        then it is looked up. Otherwise the first column with DateTime type will be used.
        """
        if time_column is None:
            datetime_indexes = (i for i, t in enumerate(types) if t == CsvColumnType.DateTime)
            time_index = next(datetime_indexes, None)
            if time_index is None:
                raise DataImportError("No time column found")
            return time_index
        else:
            self.check_has_column(time_column, headers)
            return headers.index(time_column)

    def __attr_indexes(
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

    def __metric_indexes(
        self, metrics: Optional[List[str]], headers: List[str], types: List[CsvColumnType]
    ) -> List[int]:
        if metrics is None:
            return [i for i, t in enumerate(types) if t == CsvColumnType.Numeric]
        else:
            for c in metrics:
                self.check_has_column(c, headers)
            return [headers.index(c) for c in metrics]

    def fetch_data(self, test_conf: TestConfig, selector: DataSelector = DataSelector()) -> Series:

        if not isinstance(test_conf, CsvTestConfig):
            raise ValueError("Expected CsvTestConfig")

        since_time = selector.since_time
        until_time = selector.until_time
        file = Path(test_conf.file)

        if since_time.timestamp() > until_time.timestamp():
            raise DataImportError(
                f"Invalid time range: ["
                f"{format_timestamp(int(since_time.timestamp()))}, "
                f"{format_timestamp(int(until_time.timestamp()))}]"
            )

        try:
            with open(file, newline="") as csv_file:
                reader = csv.reader(
                    csv_file,
                    delimiter=test_conf.csv_options.delimiter,
                    quotechar=test_conf.csv_options.quote_char,
                )

                headers: List[str] = next(reader, None)
                types: List[CsvColumnType] = self.__column_types(file, test_conf.csv_options)

                # Decide which columns to fetch into which components of the result:
                time_index: int = self.__time_column_index(test_conf.time_column, headers, types)
                attr_indexes: List[int] = self.__attr_indexes(selector.attributes, headers, types)
                metric_indexes: List[int] = self.__metric_indexes(selector.metrics, headers, types)
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
                    if since_time is not None and ts < since_time:
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
                                "Could not convert value in column "
                                + headers[i]
                                + ": "
                                + err.args[0]
                            )

                    # Attributes are just copied as-is, with no conversion:
                    for i in attr_indexes:
                        attributes[headers[i]].append(row[i])

                return Series(str(file.name), time, data, attributes)

        except FileNotFoundError:
            raise DataImportError(f"Input file not found: {file}")

    @staticmethod
    def __convert_time(time: str):
        try:
            return parse_datetime(time)
        except DateFormatError as err:
            raise DataImportError(err.message)

    def fetch_all_metric_names(self, test_conf: CsvTestConfig) -> List[str]:
        metrics = []
        file = Path(test_conf.name)
        with open(file, newline="") as csv_file:
            reader = csv.reader(
                csv_file,
                delimiter=test_conf.csv_options.delimiter,
                quotechar=test_conf.csv_options.quote_char,
            )

            headers: List[str] = next(reader, None)
            types: List[CsvColumnType] = self.__column_types(file, test_conf.csv_options)
            metric_indexes = self.__metric_indexes(metrics=None, headers=headers, types=types)
            for metric_index in metric_indexes:
                metrics.append(headers[metric_index])
        return metrics


class Importers:
    __config: Config
    __csv_importer: Optional[CsvImporter]
    __graphite_importer: Optional[GraphiteImporter]

    def __init__(self, config: Config):
        self.__config = config
        self.__csv_importer = None
        self.__graphite_importer = None

    def csv_importer(self) -> CsvImporter:
        if self.__csv_importer is None:
            self.__csv_importer = CsvImporter()
        return self.__csv_importer

    def graphite_importer(self) -> GraphiteImporter:
        if self.__graphite_importer is None:
            self.__graphite_importer = GraphiteImporter(Graphite(self.__config.graphite))
        return self.__graphite_importer

    def get(self, test: TestConfig) -> Importer:
        if isinstance(test, CsvTestConfig):
            return self.csv_importer()
        elif isinstance(test, GraphiteTestConfig):
            return self.graphite_importer()
        else:
            raise ValueError(f"Unsupported test type {type(test)}")
