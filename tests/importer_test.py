from datetime import datetime
from typing import Dict

import pytz

from hunter.graphite import DataSelector
from hunter.importer import CsvImporter
from hunter.test_config import create_test_config, CsvTestConfig


def create_test_conf_from_test_info(test_info: Dict) -> CsvTestConfig:
    """
    This helper method is implicitly testing the importer.create_test_config method
    """
    test_conf = create_test_config(test_info=test_info)
    assert isinstance(test_conf, CsvTestConfig), f"Did not properly create a CsvTestConfig from {test_info}"
    return test_conf


def test_import_csv():
    test_info = {"name" : "tests/resources/sample.csv"}
    test_conf = create_test_conf_from_test_info(test_info=test_info)
    importer = CsvImporter(options=test_conf.csv_options)
    test = importer.fetch(test_conf=test_conf)
    assert len(test.data.keys()) == 2
    assert len(test.time) == 10
    assert len(test.data["metric1"]) == 10
    assert len(test.data["metric2"]) == 10
    assert len(test.attributes["commit"]) == 10


def test_import_csv_with_metrics_filter():
    test_info = {"name" : "tests/resources/sample.csv"}
    test_conf = create_test_conf_from_test_info(test_info=test_info)
    importer = CsvImporter(options=test_conf.csv_options)
    selector = DataSelector()
    selector.metrics = ["metric2"]
    test = importer.fetch(test_conf=test_conf, selector=selector)
    assert len(test.data.keys()) == 1
    assert len(test.time) == 10
    assert len(test.data["metric2"]) == 10


def test_import_csv_with_time_filter():
    test_info = {"name" : "tests/resources/sample.csv"}
    test_conf = create_test_conf_from_test_info(test_info=test_info)
    importer = CsvImporter(options=test_conf.csv_options)
    selector = DataSelector()
    tz = pytz.timezone("Etc/GMT+1")
    selector.from_time = datetime(2021, 1, 5, 0, 0, 0, tzinfo=tz)
    selector.until_time = datetime(2021, 1, 7, 0, 0, 0, tzinfo=tz)
    test = importer.fetch(test_conf=test_conf, selector=selector)
    assert len(test.data.keys()) == 2
    assert len(test.time) == 2
    assert len(test.data["metric1"]) == 2
    assert len(test.data["metric2"]) == 2


def test_import_csv_with_unix_timestamps():
    test_info = {
        "name" : "tests/resources/sample.csv",
        "csv_options": {
            "time_column": "time"
        }
    }
    test_conf = create_test_conf_from_test_info(test_info=test_info)
    importer = CsvImporter(options=test_conf.csv_options)
    test = importer.fetch(test_conf=test_conf)
    assert len(test.data.keys()) == 2
    assert len(test.time) == 10
    assert len(test.data["metric1"]) == 10
    assert len(test.data["metric2"]) == 10
    ts = datetime(2021, 1, 1, 2, 0, 0, tzinfo=pytz.UTC).timestamp()
    assert test.time[0] == ts


