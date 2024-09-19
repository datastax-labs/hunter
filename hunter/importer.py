import csv
import json
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from hunter.bigquery import BigQuery
from hunter.config import Config
from hunter.data_selector import DataSelector
from hunter.graphite import DataPoint, Graphite, GraphiteError
from hunter.postgres import Postgres
from hunter.series import Metric, Series
from hunter.test_config import (
    BigQueryMetric,
    BigQueryTestConfig,
    CsvMetric,
    CsvTestConfig,
    GraphiteTestConfig,
    HistoStatTestConfig,
    JsonTestConfig,
    PostgresMetric,
    PostgresTestConfig,
    TestConfig,
)
from hunter.util import (
    DateFormatError,
    format_timestamp,
    merge_sorted,
    parse_datetime,
    resolution,
    round,
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
            attributes = test.tags.copy()
            if selector.branch:
                attributes += [selector.branch]

            # if the user has specified since_<commit/version> and/or until_<commit/version>,
            # we need to attempt to extract a timestamp from appropriate Graphite events, and
            # update selector.since_time and selector.until_time, respectively
            since_events = self.graphite.fetch_events_with_matching_time_option(
                attributes, selector.since_commit, selector.since_version
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
                attributes, selector.until_commit, selector.until_version
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
                metrics = [m for m in metrics if m.name in selector.metrics]
            path_to_metric = {test.get_path(selector.branch, m.name): m for m in metrics}
            targets = [test.get_path(selector.branch, m.name) for m in metrics]

            graphite_result = self.graphite.fetch_data(targets, selector)
            if not graphite_result:
                raise DataImportError(f"No timeseries found in Graphite for test {test.name}.")

            times = [[x.time for x in series.points] for series in graphite_result]
            time: List[int] = merge_sorted(times)[-selector.last_n_points :]

            def column(series: List[DataPoint]) -> List[float]:
                value_by_time = dict([(x.time, x.value) for x in series])
                return [value_by_time.get(t) for t in time]

            # Keep order of the keys in the result values the same as order of metrics
            values = OrderedDict()
            for m in metrics:
                values[m.name] = []
            for ts in graphite_result:
                values[path_to_metric[ts.path].name] = column(ts.points)
            for m in metrics:
                if len(values[m.name]) == 0:
                    del values[m.name]
            metrics = [m for m in metrics if m.name in values.keys()]

            events = self.graphite.fetch_events(
                attributes, selector.since_time, selector.until_time
            )
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

            attributes = {
                "run": run_ids,
                "branch": branches,
                "version": versions,
                "commit": commits,
            }
            if selector.attributes is not None:
                attributes = {a: attributes[a] for a in selector.attributes}

            metrics = {m.name: Metric(m.direction, m.scale) for m in metrics}
            return Series(
                test.name,
                branch=selector.branch,
                time=time,
                metrics=metrics,
                data=values,
                attributes=attributes,
            )

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

    @staticmethod
    def __selected_metrics(
        defined_metrics: Dict[str, CsvMetric], selected_metrics: Optional[List[str]]
    ) -> Dict[str, CsvMetric]:

        if selected_metrics is not None:
            return {name: defined_metrics[name] for name in selected_metrics}
        else:
            return defined_metrics

    def fetch_data(self, test_conf: TestConfig, selector: DataSelector = DataSelector()) -> Series:

        if not isinstance(test_conf, CsvTestConfig):
            raise ValueError("Expected CsvTestConfig")

        if selector.branch:
            raise ValueError("CSV tests don't support branching yet")

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
                metrics = self.__selected_metrics(test_conf.metrics, selector.metrics)

                # Decide which columns to fetch into which components of the result:
                try:
                    time_index: int = headers.index(test_conf.time_column)
                    attr_indexes: List[int] = [headers.index(c) for c in test_conf.attributes]
                    metric_names = [m.name for m in metrics.values()]
                    metric_columns = [m.column for m in metrics.values()]
                    metric_indexes: List[int] = [headers.index(c) for c in metric_columns]
                except ValueError as err:
                    raise DataImportError(f"Column not found {err.args[0]}")

                if time_index in attr_indexes:
                    attr_indexes.remove(time_index)
                if time_index in metric_indexes:
                    metric_indexes.remove(time_index)

                # Initialize empty lists to store the data and metadata:
                time: List[int] = []
                data: Dict[str, List[float]] = {}
                for n in metric_names:
                    data[n] = []
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
                    for name, i in zip(metric_names, metric_indexes):
                        try:
                            data[name].append(float(row[i]))
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

                # Convert metrics to series.Metrics
                metrics = {m.name: Metric(m.direction, m.scale) for m in metrics.values()}

                # Leave last n points:
                time = time[-selector.last_n_points :]
                tmp = data
                data = {}
                for k, v in tmp.items():
                    data[k] = v[-selector.last_n_points :]
                tmp = attributes
                attributes = {}
                for k, v in tmp.items():
                    attributes[k] = v[-selector.last_n_points :]

                return Series(
                    test_conf.name,
                    branch=None,
                    time=time,
                    metrics=metrics,
                    data=data,
                    attributes=attributes,
                )

        except FileNotFoundError:
            raise DataImportError(f"Input file not found: {file}")

    @staticmethod
    def __convert_time(time: str):
        try:
            return parse_datetime(time)
        except DateFormatError as err:
            raise DataImportError(err.message)

    def fetch_all_metric_names(self, test_conf: CsvTestConfig) -> List[str]:
        return [m for m in test_conf.metrics.keys()]


class HistoStatImporter(Importer):

    __TAG_METRICS = {
        "count": {"direction": 1, "scale": "1", "col": 3},
        "min": {"direction": -1, "scale": "1.0e-6", "col": 4},
        "p25": {"direction": -1, "scale": "1.0e-6", "col": 5},
        "p50": {"direction": -1, "scale": "1.0e-6", "col": 6},
        "p75": {"direction": -1, "scale": "1.0e-6", "col": 7},
        "p90": {"direction": -1, "scale": "1.0e-6", "col": 8},
        "p95": {"direction": -1, "scale": "1.0e-6", "col": 9},
        "p98": {"direction": -1, "scale": "1.0e-6", "col": 10},
        "p99": {"direction": -1, "scale": "1.0e-6", "col": 11},
        "p999": {"direction": -1, "scale": "1.0e-6", "col": 12},
        "p9999": {"direction": -1, "scale": "1.0e-6", "col": 13},
        "max": {"direction": -1, "scale": "1.0e-6", "col": 14},
    }

    @contextmanager
    def __csv_reader(self, test: HistoStatTestConfig):
        with open(Path(test.file), newline="") as csv_file:
            yield csv.reader(csv_file)

    @staticmethod
    def __parse_tag(tag: str):
        return tag.split("=")[1]

    def __get_tags(self, test: HistoStatTestConfig) -> List[str]:
        tags = set()
        with self.__csv_reader(test) as reader:
            for row in reader:
                if row[0].startswith("#"):
                    continue
                tag = self.__parse_tag(row[0])
                if tag in tags:
                    break
                tags.add(tag)
        return list(tags)

    @staticmethod
    def __metric_from_components(tag, tag_metric):
        return f"{tag}.{tag_metric}"

    @staticmethod
    def __convert_floating_point_millisecond(fpm: str) -> int:  # to epoch seconds
        return int(float(fpm) * 1000) // 1000

    def fetch_data(
        self, test: HistoStatTestConfig, selector: DataSelector = DataSelector()
    ) -> Series:
        def selected(metric_name):
            return metric_name in selector.metrics if selector.metrics is not None else True

        metrics = {}
        tag_count = 0
        for tag in self.__get_tags(test):
            tag_count += 1
            for tag_metric, attrs in self.__TAG_METRICS.items():
                if selected(self.__metric_from_components(tag, tag_metric)):
                    metrics[self.__metric_from_components(tag, tag_metric)] = Metric(
                        attrs["direction"], attrs["scale"]
                    )

        data = {k: [] for k in metrics.keys()}
        time = []
        with self.__csv_reader(test) as reader:
            start_time = None
            for row in reader:
                if not row[0].startswith("#"):
                    break
                if "StartTime" in row[0]:
                    parts = row[0].split(" ")
                    start_time = self.__convert_floating_point_millisecond(parts[1])

            if not start_time:
                raise DataImportError("No Start Time specified in HistoStat CSV comment")

            # Last iteration of row is the first non-comment row. Parse it now.
            tag_interval = 0
            while row:
                if tag_interval % tag_count == 0:
                    # Introduces a slight inaccuracy - each tag can report its interval start time
                    # with some millisecond difference. Choosing a single tag interval allows us
                    # to maintain the 'indexed by a single time variable' contract required by
                    # Series, but the time reported for almost all metrics will be _slightly_ off.
                    time.append(self.__convert_floating_point_millisecond(row[1]) + start_time)
                tag_interval += 1
                tag = self.__parse_tag(row[0])
                for tag_metric, attrs in self.__TAG_METRICS.items():
                    if selected(self.__metric_from_components(tag, tag_metric)):
                        data[self.__metric_from_components(tag, tag_metric)].append(
                            float(row[attrs["col"]])
                        )
                try:
                    row = next(reader)
                except StopIteration:
                    row = None

        # Leave last n points:
        time = time[-selector.last_n_points :]
        tmp = data
        data = {}
        for k, v in tmp.items():
            data[k] = v[-selector.last_n_points :]

        return Series(test.name, None, time, metrics, data, dict())

    def fetch_all_metric_names(self, test: HistoStatTestConfig) -> List[str]:
        metric_names = []
        for tag in self.__get_tags(test):
            for tag_metric in self.__TAG_METRICS.keys():
                metric_names.append(self.__metric_from_components(tag, tag_metric))
        return metric_names


class PostgresImporter(Importer):
    __postgres: Postgres

    def __init__(self, postgres: Postgres):
        self.__postgres = postgres

    @staticmethod
    def __selected_metrics(
        defined_metrics: Dict[str, PostgresMetric], selected_metrics: Optional[List[str]]
    ) -> Dict[str, PostgresMetric]:

        if selected_metrics is not None:
            return {name: defined_metrics[name] for name in selected_metrics}
        else:
            return defined_metrics

    def fetch_data(self, test_conf: TestConfig, selector: DataSelector = DataSelector()) -> Series:
        if not isinstance(test_conf, PostgresTestConfig):
            raise ValueError("Expected PostgresTestConfig")

        if selector.branch:
            raise ValueError("Postgres tests don't support branching yet")

        since_time = selector.since_time
        until_time = selector.until_time
        if since_time.timestamp() > until_time.timestamp():
            raise DataImportError(
                f"Invalid time range: ["
                f"{format_timestamp(int(since_time.timestamp()))}, "
                f"{format_timestamp(int(until_time.timestamp()))}]"
            )
        metrics = self.__selected_metrics(test_conf.metrics, selector.metrics)

        columns, rows = self.__postgres.fetch_data(test_conf.query)

        # Decide which columns to fetch into which components of the result:
        try:
            time_index: int = columns.index(test_conf.time_column)
            attr_indexes: List[int] = [columns.index(c) for c in test_conf.attributes]
            metric_names = [m.name for m in metrics.values()]
            metric_columns = [m.column for m in metrics.values()]
            metric_indexes: List[int] = [columns.index(c) for c in metric_columns]
        except ValueError as err:
            raise DataImportError(f"Column not found {err.args[0]}")

        time: List[float] = []
        data: Dict[str, List[float]] = {}
        for n in metric_names:
            data[n] = []
        attributes: Dict[str, List[str]] = {}
        for i in attr_indexes:
            attributes[columns[i]] = []

        for row in rows:
            ts: datetime = row[time_index]
            if since_time is not None and ts < since_time:
                continue
            if until_time is not None and ts >= until_time:
                continue
            time.append(ts.timestamp())

            # Read metric values. Note we can still fail on conversion to float,
            # because the user is free to override the column selection and thus
            # they may select a column that contains non-numeric data:
            for name, i in zip(metric_names, metric_indexes):
                try:
                    data[name].append(float(row[i]))
                except ValueError as err:
                    raise DataImportError(
                        "Could not convert value in column " + columns[i] + ": " + err.args[0]
                    )

            # Attributes are just copied as-is, with no conversion:
            for i in attr_indexes:
                attributes[columns[i]].append(row[i])

        # Convert metrics to series.Metrics
        metrics = {m.name: Metric(m.direction, m.scale) for m in metrics.values()}

        # Leave last n points:
        time = time[-selector.last_n_points :]
        tmp = data
        data = {}
        for k, v in tmp.items():
            data[k] = v[-selector.last_n_points :]
        tmp = attributes
        attributes = {}
        for k, v in tmp.items():
            attributes[k] = v[-selector.last_n_points :]

        return Series(
            test_conf.name,
            branch=None,
            time=time,
            metrics=metrics,
            data=data,
            attributes=attributes,
        )

    def fetch_all_metric_names(self, test_conf: PostgresTestConfig) -> List[str]:
        return [m for m in test_conf.metrics.keys()]


class JsonImporter(Importer):
    def __init__(self):
        self._data = {}

    @staticmethod
    def _read_json_file(filename: str):
        try:
            return json.load(open(filename))
        except FileNotFoundError:
            raise DataImportError(f"Input file not found: {filename}")

    def inputfile(self, test_conf: JsonTestConfig):
        if test_conf.file not in self._data:
            self._data[test_conf.file] = self._read_json_file(test_conf.file)
        return self._data[test_conf.file]

    def fetch_data(self, test_conf: TestConfig, selector: DataSelector = DataSelector()) -> Series:

        if not isinstance(test_conf, JsonTestConfig):
            raise ValueError("Expected JsonTestConfig")

        # TODO: refactor. THis is copy pasted from CSV importer
        since_time = selector.since_time
        until_time = selector.until_time

        if since_time.timestamp() > until_time.timestamp():
            raise DataImportError(
                f"Invalid time range: ["
                f"{format_timestamp(int(since_time.timestamp()))}, "
                f"{format_timestamp(int(until_time.timestamp()))}]"
            )

        time = []
        data = OrderedDict()
        metrics = OrderedDict()
        attributes = OrderedDict()

        for name in self.fetch_all_metric_names(test_conf):
            # Ignore metrics if selector.metrics is not None and name is not in selector.metrics
            if selector.metrics is not None and name not in selector.metrics:
                continue
            data[name] = []

        attr_names = self.fetch_all_attribute_names(test_conf)
        for name in attr_names:
            attributes[name] = []

        # If the user specified a branch, only include results from that branch.
        # Otherwise if the test config specifies a branch, only include results from that branch.
        # Else include all results.
        branch = None
        if selector.branch:
            branch = selector.branch
        elif test_conf.base_branch:
            branch = test_conf.base_branch

        objs = self.inputfile(test_conf)
        list_of_json_obj = []
        for o in objs:
            if branch and o["attributes"]["branch"] != branch:
                continue
            list_of_json_obj.append(o)

        for result in list_of_json_obj:
            time.append(result["timestamp"])
            for metric in result["metrics"]:
                # Skip metrics not in selector.metrics if selector.metrics is enabled
                if metric["name"] not in data:
                    continue

                data[metric["name"]].append(metric["value"])
                metrics[metric["name"]] = Metric(1, 1.0)
        for a in attr_names:
            attributes[a] = [o["attributes"][a] for o in list_of_json_obj]

        # Leave last n points:
        time = time[-selector.last_n_points :]
        tmp = data
        data = {}
        for k, v in tmp.items():
            data[k] = v[-selector.last_n_points :]
        tmp = attributes
        attributes = {}
        for k, v in tmp.items():
            attributes[k] = v[-selector.last_n_points :]

        return Series(
            test_conf.name,
            branch=None,
            time=time,
            metrics=metrics,
            data=data,
            attributes=attributes,
        )

    def fetch_all_metric_names(self, test_conf: JsonTestConfig) -> List[str]:
        metric_names = set()
        list_of_json_obj = self.inputfile(test_conf)
        for result in list_of_json_obj:
            for metric in result["metrics"]:
                metric_names.add(metric["name"])
        return [m for m in metric_names]

    def fetch_all_attribute_names(self, test_conf: JsonTestConfig) -> List[str]:
        attr_names = set()
        list_of_json_obj = self.inputfile(test_conf)
        for result in list_of_json_obj:
            for a in result["attributes"].keys():
                attr_names.add(a)
        return [m for m in attr_names]


class BigQueryImporter(Importer):
    __bigquery: BigQuery

    def __init__(self, bigquery: BigQuery):
        self.__bigquery = bigquery

    @staticmethod
    def __selected_metrics(
        defined_metrics: Dict[str, BigQueryMetric], selected_metrics: Optional[List[str]]
    ) -> Dict[str, BigQueryMetric]:

        if selected_metrics is not None:
            return {name: defined_metrics[name] for name in selected_metrics}
        else:
            return defined_metrics

    def fetch_data(
        self, test_conf: BigQueryTestConfig, selector: DataSelector = DataSelector()
    ) -> Series:
        if not isinstance(test_conf, BigQueryTestConfig):
            raise ValueError("Expected BigQueryTestConfig")

        if selector.branch:
            raise ValueError("BigQuery tests don't support branching yet")

        since_time = selector.since_time
        until_time = selector.until_time
        if since_time.timestamp() > until_time.timestamp():
            raise DataImportError(
                f"Invalid time range: ["
                f"{format_timestamp(int(since_time.timestamp()))}, "
                f"{format_timestamp(int(until_time.timestamp()))}]"
            )
        metrics = self.__selected_metrics(test_conf.metrics, selector.metrics)

        columns, rows = self.__bigquery.fetch_data(test_conf.query)

        # Decide which columns to fetch into which components of the result:
        try:
            time_index: int = columns.index(test_conf.time_column)
            attr_indexes: List[int] = [columns.index(c) for c in test_conf.attributes]
            metric_names = [m.name for m in metrics.values()]
            metric_columns = [m.column for m in metrics.values()]
            metric_indexes: List[int] = [columns.index(c) for c in metric_columns]
        except ValueError as err:
            raise DataImportError(f"Column not found {err.args[0]}")

        time: List[float] = []
        data: Dict[str, List[float]] = {}
        for n in metric_names:
            data[n] = []
        attributes: Dict[str, List[str]] = {}
        for i in attr_indexes:
            attributes[columns[i]] = []

        for row in rows:
            ts: datetime = row[time_index]
            if since_time is not None and ts < since_time:
                continue
            if until_time is not None and ts >= until_time:
                continue
            time.append(ts.timestamp())

            # Read metric values. Note we can still fail on conversion to float,
            # because the user is free to override the column selection and thus
            # they may select a column that contains non-numeric data:
            for name, i in zip(metric_names, metric_indexes):
                try:
                    data[name].append(float(row[i]))
                except ValueError as err:
                    raise DataImportError(
                        "Could not convert value in column " + columns[i] + ": " + err.args[0]
                    )

            # Attributes are just copied as-is, with no conversion:
            for i in attr_indexes:
                attributes[columns[i]].append(row[i])

        # Convert metrics to series.Metrics
        metrics = {m.name: Metric(m.direction, m.scale) for m in metrics.values()}

        # Leave last n points:
        time = time[-selector.last_n_points :]
        tmp = data
        data = {}
        for k, v in tmp.items():
            data[k] = v[-selector.last_n_points :]
        tmp = attributes
        attributes = {}
        for k, v in tmp.items():
            attributes[k] = v[-selector.last_n_points :]

        return Series(
            test_conf.name,
            branch=None,
            time=time,
            metrics=metrics,
            data=data,
            attributes=attributes,
        )

    def fetch_all_metric_names(self, test_conf: BigQueryTestConfig) -> List[str]:
        return [m for m in test_conf.metrics.keys()]


class Importers:
    __config: Config
    __csv_importer: Optional[CsvImporter]
    __graphite_importer: Optional[GraphiteImporter]
    __histostat_importer: Optional[HistoStatImporter]
    __postgres_importer: Optional[PostgresImporter]
    __json_importer: Optional[JsonImporter]
    __bigquery_importer: Optional[BigQueryImporter]

    def __init__(self, config: Config):
        self.__config = config
        self.__csv_importer = None
        self.__graphite_importer = None
        self.__histostat_importer = None
        self.__postgres_importer = None
        self.__json_importer = None
        self.__bigquery_importer = None

    def csv_importer(self) -> CsvImporter:
        if self.__csv_importer is None:
            self.__csv_importer = CsvImporter()
        return self.__csv_importer

    def graphite_importer(self) -> GraphiteImporter:
        if self.__graphite_importer is None:
            self.__graphite_importer = GraphiteImporter(Graphite(self.__config.graphite))
        return self.__graphite_importer

    def histostat_importer(self) -> HistoStatImporter:
        if self.__histostat_importer is None:
            self.__histostat_importer = HistoStatImporter()
        return self.__histostat_importer

    def postgres_importer(self) -> PostgresImporter:
        if self.__postgres_importer is None:
            self.__postgres_importer = PostgresImporter(Postgres(self.__config.postgres))
        return self.__postgres_importer

    def json_importer(self) -> JsonImporter:
        if self.__json_importer is None:
            self.__json_importer = JsonImporter()
        return self.__json_importer

    def bigquery_importer(self) -> BigQueryImporter:
        if self.__bigquery_importer is None:
            self.__bigquery_importer = BigQueryImporter(BigQuery(self.__config.bigquery))
        return self.__bigquery_importer

    def get(self, test: TestConfig) -> Importer:
        if isinstance(test, CsvTestConfig):
            return self.csv_importer()
        elif isinstance(test, GraphiteTestConfig):
            return self.graphite_importer()
        elif isinstance(test, HistoStatTestConfig):
            return self.histostat_importer()
        elif isinstance(test, PostgresTestConfig):
            return self.postgres_importer()
        elif isinstance(test, JsonTestConfig):
            return self.json_importer()
        elif isinstance(test, BigQueryTestConfig):
            return self.bigquery_importer()
        else:
            raise ValueError(f"Unsupported test type {type(test)}")
