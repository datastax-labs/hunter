from datetime import datetime
from typing import Dict

import pytz

from hunter.csv_options import CsvOptions
from hunter.graphite import DataSelector
from hunter.importer import CsvImporter
from hunter.test_config import create_test_config, CsvTestConfig


def test_import_csv():
    test = CsvTestConfig("test", file="tests/resources/sample.csv")
    importer = CsvImporter()
    series = importer.fetch_data(test_conf=test)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["metric1"]) == 10
    assert len(series.data["metric2"]) == 10
    assert len(series.attributes["commit"]) == 10


def test_import_csv_with_metrics_filter():
    test_conf = CsvTestConfig("test", file="tests/resources/sample.csv")
    importer = CsvImporter()
    selector = DataSelector()
    selector.metrics = ["metric2"]
    test = importer.fetch_data(test_conf, selector=selector)
    assert len(test.data.keys()) == 1
    assert len(test.time) == 10
    assert len(test.data["metric2"]) == 10


def test_import_csv_with_time_filter():
    test_conf = CsvTestConfig("test", file="tests/resources/sample.csv")
    importer = CsvImporter()
    selector = DataSelector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.since_time = datetime(2021, 1, 5, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2021, 1, 7, 0, 0, 0, tzinfo=tz)
    test = importer.fetch_data(test_conf, selector=selector)
    assert len(test.data.keys()) == 2
    assert len(test.time) == 2
    assert len(test.data["metric1"]) == 2
    assert len(test.data["metric2"]) == 2


def test_import_csv_with_unix_timestamps():
    options = CsvOptions()
    options.time_column = "time"
    test_conf = CsvTestConfig("test", file="tests/resources/sample.csv", csv_options=options)
    importer = CsvImporter()
    test = importer.fetch_data(test_conf=test_conf)
    assert len(test.data.keys()) == 2
    assert len(test.time) == 10
    assert len(test.data["metric1"]) == 10
    assert len(test.data["metric2"]) == 10
    ts = datetime(2021, 1, 1, 2, 0, 0, tzinfo=pytz.UTC).timestamp()
    assert test.time[0] == ts


def test_import_csv_semicolon_sep():
    options = CsvOptions()
    options.delimiter = ";"
    importer = CsvImporter()
    test = CsvTestConfig("test", file="tests/resources/sample-semicolons.csv", csv_options=options)
    series = importer.fetch_data(test_conf=test)
    assert len(series.data.keys()) == 2
    assert len(series.time) == 10
    assert len(series.data["metric1"]) == 10
    assert len(series.data["metric2"]) == 10
    assert len(series.attributes["commit"]) == 10
