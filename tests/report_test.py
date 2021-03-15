from hunter.performance_test import PerformanceTest
from hunter.report import Report


def test_report():
    series1 = [1.02, 0.95, 0.99, 1.00, 1.12, 0.90, 0.50, 0.51, 0.48, 0.48, 0.55]
    series2 = [2.02, 2.03, 2.01, 2.04, 1.82, 1.85, 1.79, 1.81, 1.80, 1.76, 1.78]
    time = list(range(len(series1)))
    test = PerformanceTest("test", time, {"series1": series1, "series2": series2}, {})
    changepoints = test.find_change_points()
    report = Report(test)
    output = report.format_log_annotated()
    assert "series1" in output
    assert "series2" in output
    assert "1.02" in output
    assert "0.55" in output
    assert "2.02" in output
    assert "1.78" in output
    assert "%" in output

    # 2 lines for the header
    # 1 line per each time point
    # 3 lines per each change point
    assert len(output.split("\n")) == len(time) + 2 + 3 * len(changepoints)

