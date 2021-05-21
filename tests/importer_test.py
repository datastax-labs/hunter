from datetime import datetime

import pytz

from hunter.csv_options import CsvOptions
from hunter.graphite import DataSelector
from hunter.importer import CsvImporter
from hunter.test_config import CsvTestConfig, CsvMetric


def test_import_csv():
    test = CsvTestConfig(
        name="test",
        file="tests/resources/sample.csv",
        csv_options=CsvOptions(),
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )
    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    assert len(series.attributes["commit"]) == 10


def test_import_csv_with_metrics_filter():
    test = CsvTestConfig(
        name="test",
        file="tests/resources/sample.csv",
        csv_options=CsvOptions(),
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )
    importer = CsvImporter()
    selector = DataSelector()
    selector.metrics = ["m2"]
    series = importer.fetch_data(test, selector=selector)
    assert len(series.data.keys()) == 1
    assert len(series.time) == 10
    assert len(series.data["m2"]) == 10
    assert series.metrics["m2"].scale == 5.0


def test_import_csv_with_time_filter():
    test = CsvTestConfig(
        name="test",
        file="tests/resources/sample.csv",
        csv_options=CsvOptions(),
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = CsvImporter()
    selector = DataSelector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.since_time = datetime(2021, 1, 5, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2021, 1, 7, 0, 0, 0, tzinfo=tz)
    series = importer.fetch_data(test, selector=selector)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 2
    assert len(series.data["m1"]) == 2
    assert len(series.data["m2"]) == 2


def test_import_csv_with_unix_timestamps():
    test = CsvTestConfig(
        name="test",
        file="tests/resources/sample.csv",
        csv_options=CsvOptions(),
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    ts = datetime(2021, 1, 1, 2, 0, 0, tzinfo=pytz.UTC).timestamp()
    assert series.time[0] == ts


def test_import_csv_semicolon_sep():
    options = CsvOptions()
    options.delimiter = ";"

    test = CsvTestConfig(
        name="test",
        file="tests/resources/sample-semicolons.csv",
        csv_options=options,
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )

    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["m1"]) == 10
    assert len(series.data["m2"]) == 10
    assert len(series.attributes["commit"]) == 10


def test_import_csv_last_n_points():
    test = CsvTestConfig(
        name="test",
        file="tests/resources/sample.csv",
        csv_options=CsvOptions(),
        time_column="time",
        metrics=[CsvMetric("m1", 1, 1.0, "metric1"), CsvMetric("m2", 1, 5.0, "metric2")],
        attributes=["commit"],
    )
    importer = CsvImporter()
    selector = DataSelector()
    selector.last_n_points = 5
    series = importer.fetch_data(test, selector=selector)
    assert len(series.time) == 5
    assert len(series.data["m2"]) == 5
    assert len(series.attributes["commit"]) == 5
