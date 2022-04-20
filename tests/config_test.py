from pathlib import Path

from hunter.config import load_config_from
from hunter.test_config import GraphiteTestConfig, CsvTestConfig, HistoStatTestConfig


def test_load_graphite_tests():
    config = load_config_from(Path("tests/resources/sample_config.yaml"))
    tests = config.tests
    assert len(tests) == 4
    test = tests["remote1"]
    assert isinstance(test, GraphiteTestConfig)
    assert len(test.metrics) == 7
    print(test.metrics)
    assert test.prefix == "performance_regressions.my_product.%{BRANCH}.test1"
    assert test.metrics["throughput"].name == "throughput"
    assert test.metrics["throughput"].suffix is not None
    assert test.metrics["p50"].name == "p50"
    assert test.metrics["p50"].direction == -1
    assert test.metrics["p50"].scale == 1.0e-6
    assert test.metrics["p50"].suffix is not None


def test_load_csv_tests():
    config = load_config_from(Path("tests/resources/sample_config.yaml"))
    tests = config.tests
    assert len(tests) == 4
    test = tests["local1"]
    assert isinstance(test, CsvTestConfig)
    assert len(test.metrics) == 2
    assert len(test.attributes) == 1
    assert test.file == "tests/resources/sample.csv"

    test = tests["local2"]
    assert isinstance(test, CsvTestConfig)
    assert len(test.metrics) == 2
    assert test.metrics["m1"].column == "metric1"
    assert test.metrics["m1"].direction == 1
    assert test.metrics["m2"].column == "metric2"
    assert test.metrics["m2"].direction == -1
    assert len(test.attributes) == 1
    assert test.file == "tests/resources/sample.csv"


def test_load_test_groups():
    config = load_config_from(Path("tests/resources/sample_config.yaml"))
    groups = config.test_groups
    assert len(groups) == 2
    assert len(groups["remote"]) == 2


def test_load_histostat_config():
    config = load_config_from(Path("tests/resources/histostat_test_config.yaml"))
    tests = config.tests
    assert len(tests) == 1
    test = tests["histostat-sample"]
    assert isinstance(test, HistoStatTestConfig)
    # 14 tags * 12 tag_metrics == 168 unique metrics
    assert len(test.fully_qualified_metric_names()) == 168
