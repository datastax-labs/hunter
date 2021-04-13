import collections
from dataclasses import dataclass
from typing import Dict, Optional, List, Set, OrderedDict

from hunter.csv_options import CsvOptions


@dataclass
class TestConfig:
    name: str


@dataclass
class TestConfigError(Exception):
    message: str


@dataclass
class CsvTestConfig(TestConfig):
    file: str
    time_column: Optional[str]
    csv_options: CsvOptions

    def __init__(
        self,
        name: str,
        file: str,
        time_column: Optional[str] = None,
        csv_options: Optional[CsvOptions] = CsvOptions(),
    ):
        super().__init__(name=name)
        self.file = file
        self.time_column = None
        self.csv_options = csv_options


@dataclass
class GraphiteMetric:
    name: str
    direction: int
    scale: float
    suffix: str


@dataclass
class GraphiteTestConfig(TestConfig):
    prefix: str  # location of the data for the main branch
    metrics: OrderedDict[str, GraphiteMetric]  # collection of metrics to fetch
    tags: Set[str]  # all these tags must be present on graphite events

    def get_path(self, metric_name: str) -> str:
        metric = self.metrics.get(metric_name)
        return self.prefix + "." + metric.suffix


def create_test_config(name: str, config: Dict) -> TestConfig:
    """
    Loads properties of a test from a dictionary read from hunter's config file
    This dictionary must have the `type` property to determine the type of the test.
    Other properties depend on the type.
    Currently supported test types are `fallout`, `graphite` and `csv`.
    """
    test_type = config.get("type")
    if test_type == "csv":
        return create_csv_test_config(name, config)
    elif test_type == "graphite":
        return create_graphite_test_config(name, config)
    elif test_type is None:
        raise TestConfigError(f"Test type not set for test {name}")
    else:
        raise TestConfigError(f"Unknown test type {test_type} for test {name}")


def create_csv_test_config(name: str, test_info: Dict) -> CsvTestConfig:
    csv_options = CsvOptions()
    try:
        file = test_info["file"]
    except KeyError as e:
        raise TestConfigError(f"Configuration key not found in test {name}: {e.args[0]}")
    time_column = test_info.get("time_column", "time")
    if test_info.get("csv_options"):
        csv_options.delimiter = test_info["csv_options"].get("delimiter", ",")
        csv_options.quote_char = test_info["csv_options"].get("quote_char", '"')
    return CsvTestConfig(name, file, time_column=time_column, csv_options=csv_options)


def create_graphite_test_config(name: str, test_info: Dict) -> GraphiteTestConfig:
    try:
        metrics_info = test_info["metrics"]
        if not isinstance(metrics_info, Dict):
            raise TestConfigError(f"Test {name} metrics field is not a dictionary.")
    except KeyError as e:
        raise TestConfigError(f"Configuration key not found in test {name}: {e.args[0]}")

    metrics = collections.OrderedDict()
    try:
        for (metric_name, metric_conf) in metrics_info.items():
            metrics[metric_name] = GraphiteMetric(
                name=metric_name,
                suffix=metric_conf["suffix"],
                direction=int(metric_conf.get("direction", "1")),
                scale=float(metric_conf.get("scale", "1")),
            )
    except KeyError as e:
        raise TestConfigError(f"Configuration key not found in {name}.metrics: {e.args[0]}")

    return GraphiteTestConfig(
        name=name, prefix=test_info["prefix"], tags=test_info.get("tags", []), metrics=metrics
    )
