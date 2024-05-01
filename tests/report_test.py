import json

import pytest

from hunter.analysis import ComparativeStats
from hunter.report import ChangePointReport, RegressionsReport, ReportType
from hunter.series import Metric, Series


@pytest.fixture(scope="module")
def series():
    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    return Series(
        "test",
        branch=None,
        time=time,
        metrics={"series1": Metric(1, 1.0), "series2": Metric(1, 1.0)},
        data={"series1": series1, "series2": series2},
        attributes={},
    )


@pytest.fixture(scope="module")
def change_points(series):
    return series.analyze().change_points_by_time


@pytest.fixture(scope="module")
def cp_report(series, change_points):
    return ChangePointReport(series, change_points)


def test_report(series, change_points):
    report = ChangePointReport(series, change_points)
    output = report.produce_report("test", ReportType.LOG)
    assert "series1" in output
    assert "series2" in output
    assert "1.02" in output
    assert "0.55" in output
    assert "2.02" in output
    assert "1.78" in output
    assert "-11.0%" in output
    assert "-49.4%" in output

    # 2 lines for the header
    # 1 line per each time point
    # 3 lines per each change point
    assert len(output.split("\n")) == len(series.time) + 2 + 3 * len(change_points)


def test_json_report(cp_report):
    output = cp_report.produce_report("test_name_from_config", ReportType.JSON)
    obj = json.loads(output)
    expected = {
        "test_name_from_config": [
            {
                "time": 4,
                "changes": [
                    {
                        "metric": "series2",
                        "forward_change_percent": "-11",
                        "magnitude": "0.124108",
                        "mean_after": "1.801429",
                        "mean_before": "2.025000",
                        "pvalue": "0.000000",
                        "stddev_after": "0.026954",
                        "stddev_before": "0.011180",
                    }
                ],
                "attributes": {},
            },
            {
                "time": 6,
                "changes": [
                    {
                        "metric": "series1",
                        "forward_change_percent": "-49",
                        "magnitude": "0.977513",
                        "mean_after": "0.504000",
                        "mean_before": "0.996667",
                        "pvalue": "0.000000",
                        "stddev_after": "0.025768",
                        "stddev_before": "0.067495",
                    }
                ],
                "attributes": {},
            },
        ]
    }
    assert isinstance(obj, dict)
    assert obj == expected


def test_regressions_report():
    stats = ComparativeStats(1, 2, 3, 4, 0.5)
    report = RegressionsReport([("metric1", stats)])
    output = report.produce_report("test_name", ReportType.LOG)
    assert "test_name" in output
    assert "metric1" in output
    assert "(+100.0%)" in output

    output = report.produce_report("test_name", ReportType.JSON)
    obj = json.loads(output)
    assert "test_name" in obj
    assert len(obj["test_name"]) == 1
    assert obj["test_name"][0]["metric"] == "metric1"
    assert float(obj["test_name"][0]["forward_change_percent"]) == 100.0
