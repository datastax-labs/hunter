import collections
from dataclasses import dataclass
from typing import Dict, List, Set

from hunter.csv_options import CsvOptions


@dataclass
class TestConfig:
    name: str


@dataclass
class TestConfigError(Exception):
    message: str


@dataclass
class CsvMetric:
    name: str
    direction: int
    scale: float
    column: str


@dataclass
class CsvTestConfig(TestConfig):
    file: str
    csv_options: CsvOptions
    time_column: str
    metrics: Dict[str, CsvMetric]
    attributes: List[str]

    def __init__(
        self,
        name: str,
        file: str,
        csv_options: CsvOptions = CsvOptions(),
        time_column: str = "time",
        metrics: List[CsvMetric] = None,
        attributes: List[str] = None,
    ):
        self.name = name
        self.file = file
        self.csv_options = csv_options
        self.time_column = time_column
        self.metrics = {m.name: m for m in metrics} if metrics else {}
        self.attributes = attributes if attributes else {}


@dataclass
class GraphiteMetric:
    name: str
    direction: int
    scale: float
    suffix: str
    annotate: List[str]  # tags appended to Grafana annotations


@dataclass
class GraphiteTestConfig(TestConfig):
    prefix: str  # location of the data for the main branch
    metrics: Dict[str, GraphiteMetric]  # collection of metrics to fetch
    tags: List[str]  # tags to query graphite events for this test
    annotate: List[str]  # annotation tags

    def __init__(
        self,
        name: str,
        prefix: str,
        metrics: List[GraphiteMetric],
        tags: List[str],
        annotate: List[str],
    ):
        self.name = name
        self.prefix = prefix
        self.metrics = {m.name: m for m in metrics}
        self.tags = tags
        self.annotate = annotate

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


def create_csv_test_config(test_name: str, test_info: Dict) -> CsvTestConfig:
    csv_options = CsvOptions()
    try:
        file = test_info["file"]
    except KeyError as e:
        raise TestConfigError(f"Configuration key not found in test {test_name}: {e.args[0]}")
    time_column = test_info.get("time_column", "time")
    metrics_info = test_info.get("metrics")
    metrics = []
    if isinstance(metrics_info, List):
        for name in metrics_info:
            metrics.append(CsvMetric(name, 1, 1.0, name))
    elif isinstance(metrics_info, Dict):
        for (metric_name, metric_conf) in metrics_info.items():
            metrics.append(
                CsvMetric(
                    name=metric_name,
                    column=metric_conf.get("column", metric_name),
                    direction=int(metric_conf.get("direction", "1")),
                    scale=float(metric_conf.get("scale", "1")),
                )
            )
    else:
        raise TestConfigError(f"Metrics of the test {test_name} must be a list or dictionary")

    attributes = test_info.get("attributes", [])
    if not isinstance(attributes, List):
        raise TestConfigError(f"Attributes of the test {test_name} must be a list")

    if test_info.get("csv_options"):
        csv_options.delimiter = test_info["csv_options"].get("delimiter", ",")
        csv_options.quote_char = test_info["csv_options"].get("quote_char", '"')
    return CsvTestConfig(
        test_name,
        file,
        csv_options=csv_options,
        time_column=time_column,
        metrics=metrics,
        attributes=test_info.get("attributes"),
    )


def create_graphite_test_config(name: str, test_info: Dict) -> GraphiteTestConfig:
    try:
        metrics_info = test_info["metrics"]
        if not isinstance(metrics_info, Dict):
            raise TestConfigError(f"Test {name} metrics field is not a dictionary.")
    except KeyError as e:
        raise TestConfigError(f"Configuration key not found in test {name}: {e.args[0]}")

    metrics = []
    try:
        for (metric_name, metric_conf) in metrics_info.items():
            metrics.append(
                GraphiteMetric(
                    name=metric_name,
                    suffix=metric_conf["suffix"],
                    direction=int(metric_conf.get("direction", "1")),
                    scale=float(metric_conf.get("scale", "1")),
                    annotate=metric_conf.get("annotate", []),
                )
            )
    except KeyError as e:
        raise TestConfigError(f"Configuration key not found in {name}.metrics: {e.args[0]}")

    return GraphiteTestConfig(
        name,
        prefix=test_info["prefix"],
        tags=test_info.get("tags", []),
        annotate=test_info.get("annotate", []),
        metrics=metrics,
    )
