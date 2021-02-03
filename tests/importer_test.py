from datetime import datetime
from pathlib import Path

import pytz

from hunter.graphite import DataSelector
from hunter.importer import CsvImporter, CsvOptions


def test_import_csv():
    importer = CsvImporter()
    log = importer.fetch(Path("tests/resources/sample.csv"))
    assert len(log.values.keys()) == 2
    assert len(log.time) == 10
    assert len(log.values["metric1"]) == 10
    assert len(log.values["metric2"]) == 10


def test_import_csv_with_metrics_filter():
    importer = CsvImporter()
    selector = DataSelector()
    selector.metrics = ["metric2"]
    log = importer.fetch(Path("tests/resources/sample.csv"), selector=selector)
    assert len(log.values.keys()) == 1
    assert len(log.time) == 10
    assert len(log.values["metric2"]) == 10


def test_import_csv_with_time_filter():
    importer = CsvImporter()
    selector = DataSelector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.from_time = datetime(2021, 1, 5, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2021, 1, 7, 0, 0, 0, tzinfo=tz)
    log = importer.fetch(Path("tests/resources/sample.csv"), selector=selector)
    assert len(log.values.keys()) == 2
    assert len(log.time) == 2
    assert len(log.values["metric1"]) == 2
    assert len(log.values["metric2"]) == 2


def test_import_csv_with_unix_timestamps():
    importer = CsvImporter()
    log = importer.fetch(Path("tests/resources/sample-unix-time.csv"))
    assert len(log.values.keys()) == 2
    assert len(log.time) == 10
    assert len(log.values["metric1"]) == 10
    assert len(log.values["metric2"]) == 10
    ts = datetime(2021, 1, 1, 2, 0, 0, tzinfo=pytz.UTC).timestamp()
    assert log.time[0] == ts

