from datetime import datetime

import pytz

from hunter.csv_options import CsvOptions
from hunter.graphite import DataSelector
from hunter.importer import (
    BigQueryImporter,
    CsvImporter,
    HistoStatImporter,
    PostgresImporter,
)
from hunter.test_config import (
    BigQueryMetric,
    BigQueryTestConfig,
    CsvMetric,
    CsvTestConfig,
    HistoStatTestConfig,
    PostgresMetric,
    PostgresTestConfig,
)

SAMPLE_CSV = "tests/resources/sample.csv"


def csv_test_config(file, csv_options=None):
    return CsvTestConfig(
        name="test",
        file=file,
        csv_options=csv_options if csv_options else CsvOptions(),
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )


def data_selector():
    selector = DataSelector()
    selector.since_time = datetime(1970, 1, 1, 1, 1, 1, tzinfo=pytz.UTC)
    return selector


def test_import_csv():
    test = csv_test_config(SAMPLE_CSV)
    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test, selector=data_selector())
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    assert len(series.attributes["commit"]) == 10


def test_import_csv_with_metrics_filter():
    test = csv_test_config(SAMPLE_CSV)
    importer = CsvImporter()
    selector = data_selector()
    selector.metrics = ["m2"]
    series = importer.fetch_data(test, selector=selector)
    assert len(series.data.keys()) == 1
    assert len(series.time) == 10
    assert len(series.data["m2"]) == 10
    assert series.metrics["m2"].scale == 5.0


def test_import_csv_with_time_filter():
    test = csv_test_config(SAMPLE_CSV)
    importer = CsvImporter()
    selector = data_selector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.since_time = datetime(2024, 1, 5, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2024, 1, 7, 0, 0, 0, tzinfo=tz)
    series = importer.fetch_data(test, selector=selector)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 2
    assert len(series.data["m1"]) == 2
    assert len(series.data["m2"]) == 2


def test_import_csv_with_unix_timestamps():
    test = csv_test_config(SAMPLE_CSV)
    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test, selector=data_selector())
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    ts = datetime(2024, 1, 1, 2, 0, 0, tzinfo=pytz.UTC).timestamp()
    assert series.time[0] == ts


def test_import_csv_semicolon_sep():
    options = CsvOptions()
    options.delimiter = ";"
    test = csv_test_config("tests/resources/sample-semicolons.csv", options)
    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test, selector=data_selector())
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    assert len(series.attributes["commit"]) == 10


def test_import_csv_last_n_points():
    test = csv_test_config(SAMPLE_CSV)
    importer = CsvImporter()
    selector = data_selector()
    selector.last_n_points = 5
    series = importer.fetch_data(test, selector=selector)
    assert len(series.time) == 5
    assert len(series.data["m2"]) == 5
    assert len(series.attributes["commit"]) == 5


def test_import_histostat():
    test = HistoStatTestConfig(name="test", file="tests/resources/histostat.csv")
    importer = HistoStatImporter()
    series = importer.fetch_data(test)
    assert len(series.time) == 3
    assert len(series.data["initialize.result-success.count"]) == 3


def test_import_histostat_last_n_points():
    test = HistoStatTestConfig(name="test", file="tests/resources/histostat.csv")
    importer = HistoStatImporter()
    selector = DataSelector()
    selector.last_n_points = 2
    series = importer.fetch_data(test, selector=selector)
    assert len(series.time) == 2
    assert len(series.data["initialize.result-success.count"]) == 2


class MockPostgres:
    def fetch_data(self, query: str):
        return (
            ["time", "metric1", "metric2", "commit"],
            [
                (datetime(2022, 7, 1, 15, 11, tzinfo=pytz.UTC), 2, 3, "aaabbb"),
                (datetime(2022, 7, 2, 16, 22, tzinfo=pytz.UTC), 5, 6, "cccddd"),
                (datetime(2022, 7, 3, 17, 13, tzinfo=pytz.UTC), 2, 3, "aaaccc"),
                (datetime(2022, 7, 4, 18, 24, tzinfo=pytz.UTC), 5, 6, "ccc123"),
                (datetime(2022, 7, 5, 19, 15, tzinfo=pytz.UTC), 2, 3, "aaa493"),
                (datetime(2022, 7, 6, 20, 26, tzinfo=pytz.UTC), 5, 6, "cccfgl"),
                (datetime(2022, 7, 7, 21, 17, tzinfo=pytz.UTC), 2, 3, "aaalll"),
                (datetime(2022, 7, 8, 22, 28, tzinfo=pytz.UTC), 5, 6, "cccccc"),
                (datetime(2022, 7, 9, 23, 19, tzinfo=pytz.UTC), 2, 3, "aadddd"),
                (datetime(2022, 7, 10, 9, 29, tzinfo=pytz.UTC), 5, 6, "cciiii"),
            ],
        )


def test_import_postgres():
    test = PostgresTestConfig(
        name="test",
        query="SELECT * FROM sample;",
        time_column="time",
        metrics=[PostgresMetric("m1", 1, 1.0, "metric1"), PostgresMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )
    importer = PostgresImporter(MockPostgres())
    series = importer.fetch_data(test_conf=test, selector=data_selector())
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    assert len(series.attributes["commit"]) == 10
    assert series.metrics["m2"].scale == 5.0


def test_import_postgres_with_time_filter():
    test = PostgresTestConfig(
        name="test",
        query="SELECT * FROM sample;",
        time_column="time",
        metrics=[PostgresMetric("m1", 1, 1.0, "metric1"), PostgresMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = PostgresImporter(MockPostgres())
    selector = DataSelector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.since_time = datetime(2022, 7, 8, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2022, 7, 10, 0, 0, 0, tzinfo=tz)
    series = importer.fetch_data(test, selector=selector)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 2
    assert len(series.data["m1"]) == 2
    assert len(series.data["m2"]) == 2


def test_import_postgres_last_n_points():
    test = PostgresTestConfig(
        name="test",
        query="SELECT * FROM sample;",
        time_column="time",
        metrics=[PostgresMetric("m1", 1, 1.0, "metric1"), PostgresMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = PostgresImporter(MockPostgres())
    selector = data_selector()
    selector.last_n_points = 5
    series = importer.fetch_data(test, selector=selector)
    assert len(series.time) == 5
    assert len(series.data["m2"]) == 5
    assert len(series.attributes["commit"]) == 5


class MockBigQuery:
    def fetch_data(self, query: str):
        return (
            ["time", "metric1", "metric2", "commit"],
            [
                (datetime(2022, 7, 1, 15, 11, tzinfo=pytz.UTC), 2, 3, "aaabbb"),
                (datetime(2022, 7, 2, 16, 22, tzinfo=pytz.UTC), 5, 6, "cccddd"),
                (datetime(2022, 7, 3, 17, 13, tzinfo=pytz.UTC), 2, 3, "aaaccc"),
                (datetime(2022, 7, 4, 18, 24, tzinfo=pytz.UTC), 5, 6, "ccc123"),
                (datetime(2022, 7, 5, 19, 15, tzinfo=pytz.UTC), 2, 3, "aaa493"),
                (datetime(2022, 7, 6, 20, 26, tzinfo=pytz.UTC), 5, 6, "cccfgl"),
                (datetime(2022, 7, 7, 21, 17, tzinfo=pytz.UTC), 2, 3, "aaalll"),
                (datetime(2022, 7, 8, 22, 28, tzinfo=pytz.UTC), 5, 6, "cccccc"),
                (datetime(2022, 7, 9, 23, 19, tzinfo=pytz.UTC), 2, 3, "aadddd"),
                (datetime(2022, 7, 10, 9, 29, tzinfo=pytz.UTC), 5, 6, "cciiii"),
            ],
        )


def test_import_bigquery():
    test = BigQueryTestConfig(
        name="test",
        query="SELECT * FROM sample;",
        time_column="time",
        metrics=[BigQueryMetric("m1", 1, 1.0, "metric1"), BigQueryMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )
    importer = BigQueryImporter(MockBigQuery())
    series = importer.fetch_data(test_conf=test, selector=data_selector())
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    assert len(series.attributes["commit"]) == 10
    assert series.metrics["m2"].scale == 5.0


def test_import_bigquery_with_time_filter():
    test = BigQueryTestConfig(
        name="test",
        query="SELECT * FROM sample;",
        time_column="time",
        metrics=[BigQueryMetric("m1", 1, 1.0, "metric1"), BigQueryMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = BigQueryImporter(MockBigQuery())
    selector = DataSelector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.since_time = datetime(2022, 7, 8, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2022, 7, 10, 0, 0, 0, tzinfo=tz)
    series = importer.fetch_data(test, selector=selector)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 2
    assert len(series.data["m1"]) == 2
    assert len(series.data["m2"]) == 2


def test_import_bigquery_last_n_points():
    test = BigQueryTestConfig(
        name="test",
        query="SELECT * FROM sample;",
        time_column="time",
        metrics=[BigQueryMetric("m1", 1, 1.0, "metric1"), BigQueryMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = BigQueryImporter(MockBigQuery())
    selector = data_selector()
    selector.last_n_points = 5
    series = importer.fetch_data(test, selector=selector)
    assert len(series.time) == 5
    assert len(series.data["m2"]) == 5
    assert len(series.attributes["commit"]) == 5
